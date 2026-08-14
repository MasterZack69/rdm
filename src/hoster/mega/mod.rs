//! MEGA (mega.nz) downloads.
//!
//! A MEGA link is not an HTTP URL you can hand to the normal engine. Getting
//! the bytes takes four steps:
//!
//! 1. **Parse the link.** `https://mega.nz/file/<handle>#<key>` (and the
//!    legacy `#!<handle>!<key>` form) carry a 32-byte key that never leaves
//!    the client — MEGA only ever sees `<handle>`.
//! 2. **Ask the API for a temp URL.** `POST /cs` with `{"a":"g","g":1}`
//!    returns the file size, the encrypted attributes (the real filename),
//!    and a short-lived `gfs*.userstorage.mega.co.nz` URL.
//! 3. **Range-GET that URL in parallel and decrypt.** The payload is
//!    AES-128-CTR with a 8-byte nonce; the counter is simply the 16-byte
//!    block index, so any 16-byte-aligned offset can be decrypted
//!    independently. That is what makes parallel chunks possible at all.
//! 4. **Verify.** MEGA's integrity check is a CBC-MAC over a *specific*
//!    chunk ladder, condensed into the 8-byte meta-MAC embedded in the link
//!    key. Recomputing it is the only way to know the file is intact;
//!    Content-Length matching proves nothing here.
//!
//! Folder links take a fifth step (fetching the node tree) and live in
//! [`folder`]; everything below the tree walk comes back here.
//!
//! ## What was taken from MegaBasterd, and what wasn't
//!
//! The failure modes this module guards against are the ones that MegaBasterd
//! learned the hard way:
//!
//!   * **HTTP 509 (bandwidth quota) is a download-wide fact, not a per-worker
//!     one.** When one worker sees a 509 the whole download backs off. Letting
//!     each worker discover it independently just means N workers keep
//!     hammering MEGA after the first one already knew better.
//!   * **Backoff is sliced into one-second ticks**, and is cut short when the
//!     public IP changes. A 509 usually clears the moment the user turns on a
//!     VPN; making them sit out a ten-minute exponential backoff after that is
//!     a terrible default.
//!   * **`Connection: close` on every chunk request.** Reusing a pooled socket
//!     across chunk workers is how you get a response read through a socket
//!     with leftover bytes on it — silent corruption that only shows up as a
//!     failed MAC at 100%.
//!   * **403 means the temp URL expired**, not that the file is gone. Refetch
//!     it (once per generation, not once per worker) and carry on.
//!   * **A chunk is only accepted if its length is exactly right.** A short
//!     chunk that gets written anyway shifts every later CTR offset and
//!     produces a file that is wrong from that byte onwards.
//!
//! Deliberately *not* ported: the per-chunk `.chunkN` scratch files. Those
//! exist because the Java writer is a single sequential stream. We can write
//! decrypted plaintext straight into the sparse `.mctemp` at its final offset,
//! so resume state is just "which tasks finished", kept in a small sidecar.
//!
//! MegaBasterd (<https://github.com/tonikelope/megabasterd>, GPLv3) is prior
//! art for the behaviour above, not a source of code: everything here is
//! original Rust.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use aes::Aes128;
use aes::cipher::{BlockCipherDecrypt, BlockCipherEncrypt, KeyInit};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::StreamExt;
use futures_util::stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::ui::{ProgressSink, SlotState, format_size};

/// Folder links: node tree, key unwrapping, and per-file orchestration.
pub mod folder;

/// MEGA's API endpoint. `g.api` is the one that hands out download URLs.
const API_URL: &str = "https://g.api.mega.co.nz/cs";

/// Where we look up our own public IP to notice a VPN coming up mid-509.
const PUBLIC_IP_URL: &str = "https://api.ipify.org";

/// Default parallel chunk workers. MEGA starts throttling well before this
/// buys anything, and more slots means more sockets sharing one quota.
pub const WORKERS_DEFAULT: usize = 6;

/// How many 1 MiB MAC chunks one range request covers. Straight from
/// MegaBasterd: big enough that per-request overhead disappears, small enough
/// that losing one to a timeout is cheap.
pub const CHUNK_SIZE_MULTI: u64 = 20;

/// Longest single backoff between chunk attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// How often to re-check the public IP while sitting in a 509 backoff.
const IP_RECHECK_TICKS: u64 = 30;

/// Give up refetching an expired temp URL after this many tries.
const MAX_URL_RETRY: u32 = 32;

/// Read buffer per worker.
const BUFFER_SIZE: usize = 64 * 1024;

/// How long a worker parked on someone else's 509 waits between re-checks.
const QUOTA_POLL: Duration = Duration::from_millis(500);

// ── Link parsing ─────────────────────────────────────────────────────

/// Does this look like something only the MEGA path can handle?
pub fn is_mega_url(url: &str) -> bool {
    let lower = url.trim().to_ascii_lowercase();
    let host = lower
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(&lower)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("");
    host == "mega.nz"
        || host == "mega.co.nz"
        || host.ends_with(".mega.nz")
        || host.ends_with(".mega.co.nz")
}

/// Everything after `mega.nz/`, with the scheme and host removed.
///
/// Shared with the folder parser so both agree on where a link's path starts.
pub(crate) fn link_path(url: &str) -> &str {
    url.split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(url)
        .split_once('/')
        .map(|(_, rest)| rest)
        .unwrap_or("")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MegaLink {
    /// The public file handle MEGA knows about.
    pub handle: String,
    /// The base64 key fragment. Never sent to MEGA.
    pub key: String,
}

/// Splits a MEGA link into handle + key.
///
/// Both layouts are accepted because plenty of links in the wild are still the
/// pre-2016 form:
///
/// ```text
/// https://mega.nz/file/AbCdEfGh#key
/// https://mega.nz/#!AbCdEfGh!key
/// ```
pub fn parse_link(url: &str) -> Result<MegaLink> {
    let url = url.trim();
    if !is_mega_url(url) {
        bail!("Not a mega.nz link: {url}");
    }

    let after_host = link_path(url);

    // Folder links go through `folder::download_folder`, which needs the node
    // tree before it has anything resembling a file handle.
    if folder::is_folder_link(url) {
        bail!("This is a MEGA folder link — it is handled by the folder path, not parse_link");
    }

    let (handle, key) = if let Some(rest) = after_host.strip_prefix("file/") {
        let (handle, key) = rest
            .split_once('#')
            .ok_or_else(|| anyhow!("MEGA link has no decryption key after '#'"))?;
        (handle, key)
    } else if let Some(rest) = after_host.strip_prefix("#!") {
        let (handle, key) = rest
            .split_once('!')
            .ok_or_else(|| anyhow!("MEGA link has no decryption key after '!'"))?;
        (handle, key)
    } else {
        bail!("Unrecognised MEGA link shape: {url}");
    };

    let handle = handle.trim().to_owned();
    // A trailing query or fragment on the key is common when links are pasted
    // out of a browser address bar.
    let key = key
        .split(['?', '&', '/'])
        .next()
        .unwrap_or("")
        .trim().to_owned();

    if handle.is_empty() || key.is_empty() {
        bail!("MEGA link is missing its handle or key");
    }

    Ok(MegaLink { handle, key })
}

// ── Key material ───────────────────────────────────────────────────

/// The three things packed into a file link's 32-byte key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileKey {
    /// AES-128 key, used for both CTR decryption and the CBC-MAC.
    pub aes: [u8; 16],
    /// CTR nonce (the high half of every counter block).
    pub nonce: [u8; 8],
    /// Expected condensed MAC of the plaintext.
    pub meta_mac: [u8; 8],
}

/// URL-safe base64, tolerant of the standard alphabet and stray padding —
/// links get mangled by chat clients constantly.
pub(crate) fn b64_decode(input: &str) -> Result<Vec<u8>> {
    let normalised: String = input
        .trim()
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '=')
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            other => other,
        })
        .collect();
    URL_SAFE_NO_PAD
        .decode(normalised.as_bytes())
        .context("MEGA key is not valid base64")
}

/// Unpacks the 32 raw bytes of a file key.
///
/// Four 64-bit words: `k0 k1 | nonce | meta_mac`. The actual AES key is the
/// *xor* of the first and second halves — MEGA stores the key obfuscated by
/// the very MAC it protects. Folder nodes carry these same 32 bytes inside
/// their node key, which is why this is split out from the link parser.
pub fn file_key_from_bytes(raw: &[u8]) -> Result<FileKey> {
    if raw.len() < 32 {
        bail!(
            "MEGA file key must be 32 bytes, got {} — is this a folder key?",
            raw.len()
        );
    }

    let mut aes = [0u8; 16];
    for i in 0..16 {
        aes[i] = raw[i] ^ raw[i + 16];
    }
    let mut nonce = [0u8; 8];
    nonce.copy_from_slice(&raw[16..24]);
    let mut meta_mac = [0u8; 8];
    meta_mac.copy_from_slice(&raw[24..32]);

    Ok(FileKey {
        aes,
        nonce,
        meta_mac,
    })
}

/// Decodes the `#key` fragment of a file link.
pub fn decode_file_key(key_b64: &str) -> Result<FileKey> {
    let raw = b64_decode(key_b64)?;
    if raw.len() < 32 {
        bail!(
            "MEGA file key must decode to 32 bytes, got {} — is this a folder link?",
            raw.len()
        );
    }
    file_key_from_bytes(&raw)
}

/// The CTR IV for a byte offset: nonce, then the 16-byte block index.
///
/// Offsets are always 16-byte aligned (every MEGA chunk boundary is), which is
/// exactly why chunks can be fetched out of order.
fn ctr_iv(nonce: &[u8; 8], offset: u64) -> [u8; 16] {
    let mut iv = [0u8; 16];
    iv[..8].copy_from_slice(nonce);
    iv[8..].copy_from_slice(&(offset / 16).to_be_bytes());
    iv
}

// ── Chunk ladder ───────────────────────────────────────────────────

/// MEGA's MAC chunk boundaries: `(offset, len)` pairs.
///
/// Sizes ramp 128K, 256K … 896K for the first seven chunks (3584 KiB total)
/// and are 1 MiB after that. This ladder is not a tunable — the CBC-MAC is
/// defined over these exact spans, so getting it wrong makes every file fail
/// verification even when the bytes are perfect.
pub fn mac_chunks(size: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut offset = 0u64;
    let mut index = 1u64;
    while offset < size {
        let len = if index <= 7 {
            index * 128 * 1024
        } else {
            1024 * 1024
        };
        let len = len.min(size - offset);
        out.push((offset, len));
        offset += len;
        index += 1;
    }
    out
}

/// One range request: a run of consecutive MAC chunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: usize,
    pub offset: u64,
    pub len: u64,
}

/// Groups the MAC ladder into range requests of roughly `target` bytes.
///
/// Tasks never split a MAC chunk, so verification can stay chunk-aligned no
/// matter how the work was divided.
pub fn plan_tasks(chunks: &[(u64, u64)], target: u64) -> Vec<Task> {
    let target = target.max(1);
    let mut tasks: Vec<Task> = Vec::new();
    let mut i = 0;
    while i < chunks.len() {
        let offset = chunks[i].0;
        let mut len = 0u64;
        while i < chunks.len() && (len == 0 || len + chunks[i].1 <= target) {
            len += chunks[i].1;
            i += 1;
        }
        tasks.push(Task {
            id: tasks.len(),
            offset,
            len,
        });
    }
    tasks
}

// ── Block cipher ───────────────────────────────────────────────────

fn encrypt_block(cipher: &Aes128, block: &mut [u8; 16]) {
    let mut b = aes::Block::from(*block);
    cipher.encrypt_block(&mut b);
    block.copy_from_slice(&b);
}

fn decrypt_block(cipher: &Aes128, block: &mut [u8; 16]) {
    let mut b = aes::Block::from(*block);
    cipher.decrypt_block(&mut b);
    block.copy_from_slice(&b);
}

/// AES-128-ECB decrypt, in place, over whole blocks.
///
/// ECB is indefensible for data, but MEGA uses it for key wrapping, where
/// every "message" is exactly one or two blocks of high-entropy key material.
pub(crate) fn ecb_decrypt(key: &[u8; 16], data: &[u8]) -> Vec<u8> {
    let cipher = Aes128::new(key.into());
    let mut out = Vec::with_capacity(data.len());
    for block_start in (0..data.len() / 16 * 16).step_by(16) {
        let mut block = [0u8; 16];
        block.copy_from_slice(&data[block_start..block_start + 16]);
        decrypt_block(&cipher, &mut block);
        out.extend_from_slice(&block);
    }
    out
}

// ── CTR ──────────────────────────────────────────────────────────

/// AES-128-CTR with a big-endian 128-bit counter.
///
/// This used to be `ctr::Ctr128BE<Aes128>`, and is written out here because
/// the `ctr` crate has no release that works with a current `aes`: the latest
/// stable `ctr` (0.9.2, 2022) is built on `cipher` 0.4, while `aes` 0.9 is on
/// `cipher` 0.5. Depending on a pre-release to bridge that, in the one code
/// path where a subtle error means silently corrupt files, is a worse trade
/// than the forty lines below.
///
/// To be clear about what is and is not being hand-rolled: AES itself is
/// still the `aes` crate. CTR mode on top of it is a counter, a block
/// encryption and an XOR. The tests pin it to the NIST SP 800-38A CTR-AES128
/// vectors, so "it matches `Ctr128BE`" is checked rather than assumed.
struct Aes128Ctr {
    cipher: Aes128,
    counter: [u8; 16],
    /// Keystream for `counter - 1`, i.e. the block currently being consumed.
    keystream: [u8; 16],
    /// How much of `keystream` is spent. Starts "empty" so the first byte
    /// forces a refill.
    used: usize,
}

impl Aes128Ctr {
    fn new(key: &[u8; 16], iv: &[u8; 16]) -> Self {
        Self {
            cipher: Aes128::new(key.into()),
            counter: *iv,
            keystream: [0u8; 16],
            used: 16,
        }
    }

    /// Encrypts the current counter block and increments it.
    fn refill(&mut self) {
        self.keystream = self.counter;
        encrypt_block(&self.cipher, &mut self.keystream);

        // Big-endian increment across the whole 16 bytes, so a counter that
        // rolls over its low word carries into the nonce exactly as Ctr128BE
        // does. MEGA never gets near that boundary in practice (it needs a
        // file of 2^64 blocks), but matching the reference behaviour costs
        // nothing and means the NIST vectors below are a real test.
        for byte in self.counter.iter_mut().rev() {
            let (next, carried) = byte.overflowing_add(1);
            *byte = next;
            if !carried {
                break;
            }
        }

        self.used = 0;
    }

    /// XORs the keystream over `data`, in place.
    ///
    /// Byte-wise on purpose: the caller feeds this whatever the socket
    /// returned, which is rarely a multiple of the block size, and the
    /// keystream has to carry across those boundaries.
    fn apply_keystream(&mut self, data: &mut [u8]) {
        for byte in data.iter_mut() {
            if self.used == 16 {
                self.refill();
            }
            *byte ^= self.keystream[self.used];
            self.used += 1;
        }
    }
}

// ── CBC-MAC ──────────────────────────────────────────────────────

/// Rolling CBC-MAC over one MAC chunk of plaintext.
///
/// Seeded with the nonce twice over, per MEGA's scheme, and the final partial
/// block is zero-padded.
struct ChunkMac {
    cipher: Aes128,
    state: [u8; 16],
    pending: [u8; 16],
    filled: usize,
}

impl ChunkMac {
    fn new(key: &FileKey) -> Self {
        let mut state = [0u8; 16];
        state[..8].copy_from_slice(&key.nonce);
        state[8..].copy_from_slice(&key.nonce);
        Self {
            cipher: Aes128::new(&key.aes.into()),
            state,
            pending: [0u8; 16],
            filled: 0,
        }
    }

    fn update(&mut self, data: &[u8]) {
        for &byte in data {
            self.pending[self.filled] = byte;
            self.filled += 1;
            if self.filled == 16 {
                self.absorb();
            }
        }
    }

    fn absorb(&mut self) {
        for i in 0..16 {
            self.state[i] ^= self.pending[i];
        }
        let cipher = &self.cipher;
        let mut state = self.state;
        encrypt_block(cipher, &mut state);
        self.state = state;
        self.pending = [0u8; 16];
        self.filled = 0;
    }

    fn finish(mut self) -> [u8; 16] {
        if self.filled > 0 {
            // Remaining bytes of `pending` are already zero.
            self.absorb();
        }
        self.state
    }
}

/// Folds per-chunk MACs into the 8-byte meta-MAC stored in the link.
pub fn combine_macs(key: &FileKey, chunk_macs: &[[u8; 16]]) -> [u8; 8] {
    let cipher = Aes128::new(&key.aes.into());
    let mut file_mac = [0u8; 16];
    for mac in chunk_macs {
        for i in 0..16 {
            file_mac[i] ^= mac[i];
        }
        encrypt_block(&cipher, &mut file_mac);
    }

    let mut out = [0u8; 8];
    for i in 0..4 {
        out[i] = file_mac[i] ^ file_mac[i + 4];
        out[i + 4] = file_mac[i + 8] ^ file_mac[i + 12];
    }
    out
}

/// Reads the finished plaintext back off disk and recomputes the meta-MAC.
///
/// Re-reading rather than trusting MACs accumulated during download is the
/// point: it catches a bad write, a truncated flush, or anything else that
/// happened between "we decrypted it" and "it is on disk".
pub async fn verify_file(
    path: &Path,
    key: &FileKey,
    size: u64,
    cancel: &CancellationToken,
    sink: &Arc<dyn ProgressSink>,
) -> Result<bool> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("Cannot reopen {} to verify it", path.display()))?;

    let mut macs = Vec::new();
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut verified = 0u64;

    for (_, len) in mac_chunks(size) {
        if cancel.is_cancelled() {
            return Ok(false);
        }
        let mut mac = ChunkMac::new(key);
        let mut left = len;
        while left > 0 {
            let want = left.min(buffer.len() as u64) as usize;
            let read = file.read(&mut buffer[..want]).await?;
            if read == 0 {
                bail!("File ended early while verifying — expected {size} bytes");
            }
            mac.update(&buffer[..read]);
            left -= read as u64;
            verified += read as u64;
        }
        macs.push(mac.finish());
        sink.progress(verified);
    }

    Ok(combine_macs(key, &macs) == key.meta_mac)
}

// ── API ──────────────────────────────────────────────────────────

/// Human wording for the MEGA API error codes worth distinguishing.
pub fn api_error_message(code: i64) -> String {
    match code {
        -1 => "MEGA internal error (-1)".into(),
        -2 => "MEGA rejected the request as malformed (-2)".into(),
        -3 | -4 => "MEGA is rate limiting this client (-3/-4) — try again shortly".into(),
        -6 => "Too many concurrent connections to MEGA (-6)".into(),
        -8 => "This MEGA link has expired (-8)".into(),
        -9 => "MEGA link not found — wrong link, or the file was removed (-9)".into(),
        -11 => "No access to this MEGA file (-11)".into(),
        -13 => "MEGA says this is a folder, not a file (-13)".into(),
        -14 => "Wrong decryption key for this MEGA link (-14)".into(),
        -15 => "MEGA session expired (-15)".into(),
        -16 => "This MEGA file is blocked or has been deleted (-16)".into(),
        -17 => "MEGA bandwidth quota exceeded (-17)".into(),
        -18 => "MEGA file temporarily unavailable (-18) — retry in a moment".into(),
        other => format!("MEGA API error {other}"),
    }
}

/// Errors that will never clear no matter how long we wait.
pub fn is_fatal_api_error(code: i64) -> bool {
    matches!(code, -2 | -8 | -9 | -11 | -13 | -14 | -15 | -16)
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub size: u64,
    pub name: Option<String>,
    pub url: String,
}

#[derive(Debug, Deserialize)]
struct GetResponse {
    #[serde(default)]
    s: Option<u64>,
    #[serde(default)]
    g: Option<String>,
    #[serde(default)]
    at: Option<String>,
    #[serde(default)]
    e: Option<i64>,
}

/// Talks to `/cs`. Keeps its own sequence counter, which MEGA uses to dedupe
/// retried requests.
pub(crate) struct MegaApi {
    client: Client,
    seq: AtomicU64,
}

impl MegaApi {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            seq: AtomicU64::new(rand_seq()),
        }
    }

    /// POSTs one command and returns the first element of the result array.
    ///
    /// `folder` scopes the request to a public folder share. That scope is a
    /// query parameter (`n`), not part of the command, and it changes what
    /// handles inside the command mean — which is why it is threaded through
    /// everywhere rather than baked into a second API type.
    pub(crate) async fn call(
        &self,
        command: serde_json::Value,
        folder: Option<&str>,
    ) -> Result<serde_json::Value> {
        let id = self.seq.fetch_add(1, Ordering::Relaxed);

        // Built by hand rather than with `RequestBuilder::query`, which lives
        // behind reqwest's urlencoded feature. `id` is a u64 and a folder
        // handle is base64url, so there is nothing here that needs escaping.
        let mut endpoint = format!("{API_URL}?id={id}");
        if let Some(folder) = folder {
            endpoint.push_str("&n=");
            endpoint.push_str(folder);
        }

        let response = self
            .client
            .post(&endpoint)
            .json(&json!([command]))
            .send()
            .await
            .context("Cannot reach the MEGA API")?;

        let status = response.status();
        let text = response.text().await.context("Empty MEGA API response")?;

        if !status.is_success() {
            bail!("MEGA API returned HTTP {status}");
        }

        // The API answers a bare negative number for whole-request failures
        // and an array of results otherwise.
        let trimmed = text.trim();
        if let Ok(code) = trimmed.parse::<i64>() {
            bail!(api_error_message(code));
        }

        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(trimmed).context("Unexpected MEGA API response shape")?;
        let first = parsed
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("MEGA API returned no result"))?;

        if let Some(code) = first.as_i64() {
            bail!(api_error_message(code));
        }

        Ok(first)
    }

    /// Fetches a temp download URL plus metadata for `handle`.
    ///
    /// Inside a folder share the node is addressed by `n`; a standalone file
    /// link uses `p`. Sending the wrong one gets you -9, not a clear error.
    async fn fetch(&self, handle: &str, key: &FileKey, folder: Option<&str>) -> Result<FileInfo> {
        let command = if folder.is_some() {
            json!({ "a": "g", "g": 1, "n": handle })
        } else {
            json!({ "a": "g", "g": 1, "p": handle })
        };

        let first = self.call(command, folder).await?;

        let result: GetResponse =
            serde_json::from_value(first).context("Unexpected MEGA API result shape")?;

        if let Some(code) = result.e {
            bail!(api_error_message(code));
        }

        let size = result
            .s
            .ok_or_else(|| anyhow!("MEGA did not report a file size"))?;
        let url = result
            .g
            .ok_or_else(|| anyhow!("MEGA did not return a download URL"))?;
        let name = result
            .at
            .as_deref()
            .and_then(|at| decrypt_attributes(at, &key.aes));

        Ok(FileInfo { size, name, url })
    }
}

/// Decrypts an `at` blob (AES-128-CBC, zero IV) and pulls `n` out of it.
///
/// Takes a bare AES key rather than a [`FileKey`] because folder nodes have
/// attributes too, and a folder's key is 16 bytes with no nonce or MAC.
///
/// Best effort by design: a garbled filename is not a reason to refuse to
/// download the file, we just fall back to the handle.
pub(crate) fn decrypt_attributes(at: &str, aes: &[u8; 16]) -> Option<String> {
    let mut data = b64_decode(at).ok()?;
    if data.len() < 16 || data.len() % 16 != 0 {
        return None;
    }

    let cipher = Aes128::new(aes.into());
    let mut previous = [0u8; 16];
    for block_start in (0..data.len()).step_by(16) {
        let mut block = [0u8; 16];
        block.copy_from_slice(&data[block_start..block_start + 16]);
        let ciphertext = block;
        decrypt_block(&cipher, &mut block);
        for i in 0..16 {
            block[i] ^= previous[i];
        }
        previous = ciphertext;
        data[block_start..block_start + 16].copy_from_slice(&block);
    }

    let text = String::from_utf8_lossy(&data);
    let text = text.trim_end_matches('\0');
    let json_part = text.strip_prefix("MEGA")?;
    let value: serde_json::Value = serde_json::from_str(json_part.trim()).ok()?;
    let name = value.get("n")?.as_str()?.to_owned();
    Some(sanitize_filename(&name))
}

/// Keeps a MEGA-supplied name from escaping the download directory.
///
/// The filename comes from inside an encrypted blob controlled by whoever
/// uploaded the file, so `../../.bashrc` is entirely possible.
///
/// Note what this does *not* do: strip leading dots. `.bashrc` is a perfectly
/// ordinary filename, and rewriting it to `bashrc` would quietly write a
/// different file than the one the user asked for. Only a name made entirely
/// of dots (`.`, `..`) is traversal, and those are replaced wholesale.
pub fn sanitize_filename(name: &str) -> String {
    let base = name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(name)
        .trim();

    let base = if base.chars().all(|c| c == '.') { "" } else { base };

    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '\0' | '/' | '\\') {
                '_'
            } else {
                c
            }
        })
        .collect();

    if cleaned.is_empty() {
        "mega-download".to_owned()
    } else {
        cleaned
    }
}

fn rand_seq() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1)
}

// ── Resume sidecar ────────────────────────────────────────────────

/// Which range requests already landed, so a rerun does not redo them.
///
/// Deliberately keyed by handle *and* size: a link that now points at a
/// different file must not be resumed on top of the old bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeState {
    pub handle: String,
    pub size: u64,
    pub target: u64,
    pub done: BTreeSet<usize>,
}

impl ResumeState {
    fn fresh(handle: &str, size: u64, target: u64) -> Self {
        Self {
            handle: handle.to_owned(),
            size,
            target,
            done: BTreeSet::new(),
        }
    }

    fn load(path: &Path, handle: &str, size: u64, target: u64) -> Self {
        let parsed = std::fs::read_to_string(path)
            .ok()
            .and_then(|text| serde_json::from_str::<ResumeState>(&text).ok());

        match parsed {
            Some(state) if state.handle == handle && state.size == size && state.target == target => {
                state
            }
            _ => Self::fresh(handle, size, target),
        }
    }

    fn save(&self, path: &Path) {
        if let Ok(text) = serde_json::to_string(self) {
            let _ = std::fs::write(path, text);
        }
    }
}

// ── Download ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MegaOptions {
    pub workers: usize,
    pub verify_mac: bool,
    pub resume_on_ip_change: bool,
    pub max_retries: u32,
    pub overwrite: bool,
}

impl Default for MegaOptions {
    fn default() -> Self {
        Self {
            workers: WORKERS_DEFAULT,
            verify_mac: true,
            resume_on_ip_change: true,
            max_retries: 6,
            overwrite: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MegaOutcome {
    Completed { path: PathBuf, bytes: u64 },
    AlreadyPresent { path: PathBuf },
    Cancelled { path: PathBuf },
}

/// Everything the workers share. One instance per download.
struct Shared {
    api: MegaApi,
    client: Client,
    handle: String,
    /// Public folder share this node belongs to, if any.
    folder: Option<String>,
    key: FileKey,
    size: u64,
    part_path: PathBuf,
    state_path: PathBuf,
    options: MegaOptions,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
    downloaded: AtomicU64,
    /// Set by whichever worker sees a 509 first; read by all of them so the
    /// whole download backs off together instead of one slot at a time.
    quota_hit: AtomicBool,
    /// Temp URL plus its generation. A worker that gets a 403 asks for the
    /// next generation; concurrent 403s on the same generation share one
    /// refetch instead of stampeding the API.
    url: Mutex<(String, u64)>,
    state: Mutex<ResumeState>,
}

impl Shared {
    async fn current_url(&self) -> (String, u64) {
        self.url.lock().await.clone()
    }

    /// Refetches the temp URL unless someone already did it for `generation`.
    async fn refresh_url(&self, generation: u64) -> Result<String> {
        let mut guard = self.url.lock().await;
        if guard.1 != generation {
            return Ok(guard.0.clone());
        }

        let mut attempt = 0u32;
        loop {
            if self.cancel.is_cancelled() {
                return Ok(guard.0.clone());
            }
            match self
                .api
                .fetch(&self.handle, &self.key, self.folder.as_deref())
                .await
            {
                Ok(info) => {
                    *guard = (info.url.clone(), generation + 1);
                    return Ok(info.url);
                }
                Err(err) => {
                    attempt += 1;
                    if attempt >= MAX_URL_RETRY {
                        return Err(err.context("Gave up refreshing the MEGA download URL"));
                    }
                    self.sink
                        .note(&format!("MEGA link refresh failed ({err}); retrying"));
                    tokio::time::sleep(backoff_delay(attempt)).await;
                }
            }
        }
    }

    /// Parks a worker for as long as another worker is sitting out a 509.
    ///
    /// The quota is per-IP, so it applies to every slot at once. Without this
    /// the flag would be write-only: the other five workers would each go and
    /// collect their own 509, which is both pointless and the thing most
    /// likely to get an IP temp-banned.
    async fn wait_out_quota(&self) {
        while self.quota_hit.load(Ordering::Relaxed) {
            if self.cancel.is_cancelled() {
                return;
            }
            tokio::time::sleep(QUOTA_POLL).await;
        }
    }

    async fn mark_done(&self, task_id: usize) {
        let mut state = self.state.lock().await;
        state.done.insert(task_id);
        state.save(&self.state_path);
    }

    fn add_progress(&self, bytes: u64) {
        let total = self.downloaded.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.sink.progress(total);
    }
}

/// Exponential backoff, capped. Attempt numbers start at 1.
fn backoff_delay(attempt: u32) -> Duration {
    let secs = 1u64 << attempt.min(6);
    Duration::from_secs(secs).min(MAX_BACKOFF)
}

async fn public_ip(client: &Client) -> Option<String> {
    let response = client
        .get(PUBLIC_IP_URL)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .ok()?;
    let text = response.text().await.ok()?;
    let text = text.trim().to_owned();
    (!text.is_empty()).then_some(text)
}

/// Sleeps out a backoff in one-second ticks, cutting it short when the public
/// IP changes.
///
/// This is the single most useful thing MegaBasterd does about quota: a 509
/// clears the instant the user's IP changes, so noticing that within ~30s
/// beats waiting out a backoff that no longer applies.
async fn quota_backoff(shared: &Shared, attempt: u32, quota: bool) -> bool {
    let total = backoff_delay(attempt).as_secs().max(1);
    let watch_ip = quota && shared.options.resume_on_ip_change;

    let start_ip = if watch_ip {
        public_ip(&shared.client).await
    } else {
        None
    };

    if quota {
        shared.sink.note(&format!(
            "MEGA bandwidth quota (HTTP 509) — all slots waiting {total}s{}",
            if start_ip.is_some() {
                ", will resume early if your IP changes"
            } else {
                ""
            }
        ));
    }

    for tick in 0..total {
        if shared.cancel.is_cancelled() {
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        if let Some(ref before) = start_ip
            && (tick + 1) % IP_RECHECK_TICKS == 0
            && let Some(now) = public_ip(&shared.client).await
            && &now != before
        {
            shared
                .sink
                .note("Public IP changed — retrying MEGA download now");
            return true;
        }
    }
    false
}

/// Fetches, decrypts and writes one task, retrying until it works or the
/// retry budget runs out.
async fn run_task(shared: Arc<Shared>, task: Task) -> Result<()> {
    let mut attempt = 0u32;

    loop {
        if shared.cancel.is_cancelled() {
            return Ok(());
        }

        // Somebody else may be in the middle of a quota backoff.
        shared.wait_out_quota().await;

        let (url, generation) = shared.current_url().await;
        match attempt_task(&shared, &task, &url).await {
            Ok(()) => {
                shared.mark_done(task.id).await;
                return Ok(());
            }
            Err(TaskError::Expired) => {
                // Not a real failure and not worth a retry slot: the temp URL
                // simply aged out, which happens on any long download.
                shared.refresh_url(generation).await?;
            }
            Err(TaskError::Quota) => {
                attempt += 1;
                // Whoever raises the flag owns the wait and owns clearing it.
                // Every other worker parks in `wait_out_quota` meanwhile, so
                // exactly one of them is talking to MEGA during a 509.
                if !shared.quota_hit.swap(true, Ordering::Relaxed) {
                    let woke_early = quota_backoff(&shared, attempt, true).await;
                    shared.quota_hit.store(false, Ordering::Relaxed);
                    if woke_early {
                        attempt = 0;
                    }
                }
            }
            Err(TaskError::Fatal(err)) => return Err(err),
            Err(TaskError::Transient(err)) => {
                attempt += 1;
                if attempt > shared.options.max_retries {
                    return Err(err.context(format!(
                        "MEGA chunk at offset {} failed after {} attempts",
                        task.offset, shared.options.max_retries
                    )));
                }
                shared
                    .sink
                    .note(&format!("MEGA chunk retry {attempt}: {err}"));
                quota_backoff(&shared, attempt, false).await;
            }
        }
    }
}

enum TaskError {
    /// HTTP 403: the temp URL expired.
    Expired,
    /// HTTP 509: bandwidth quota.
    Quota,
    /// Worth another try.
    Transient(anyhow::Error),
    /// Never going to work.
    Fatal(anyhow::Error),
}

async fn attempt_task(shared: &Shared, task: &Task, url: &str) -> Result<(), TaskError> {
    let end = task.offset + task.len - 1;
    let chunk_url = format!("{}/{}-{}", url.trim_end_matches('/'), task.offset, end);

    let response = shared
        .client
        .get(&chunk_url)
        // Never share a socket between chunk workers: a response read through
        // a connection with leftover bytes on it is silent corruption that
        // only surfaces as a MAC failure at the very end.
        .header(reqwest::header::CONNECTION, "close")
        .send()
        .await
        .map_err(|e| TaskError::Transient(e.into()))?;

    let status = response.status();
    if status == reqwest::StatusCode::FORBIDDEN {
        return Err(TaskError::Expired);
    }
    if status.as_u16() == 509 {
        return Err(TaskError::Quota);
    }
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::GONE {
        return Err(TaskError::Fatal(anyhow!(
            "MEGA storage node says this file is gone (HTTP {status})"
        )));
    }
    if !status.is_success() {
        return Err(TaskError::Transient(anyhow!("HTTP {status}")));
    }

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&shared.part_path)
        .await
        .map_err(|e| TaskError::Fatal(e.into()))?;
    file.seek(std::io::SeekFrom::Start(task.offset))
        .await
        .map_err(|e| TaskError::Fatal(e.into()))?;

    let mut cipher = Aes128Ctr::new(
        &shared.key.aes,
        &ctr_iv(&shared.key.nonce, task.offset),
    );

    let mut written = 0u64;
    let mut body = response.bytes_stream();

    while let Some(piece) = body.next().await {
        if shared.cancel.is_cancelled() {
            return Err(TaskError::Fatal(anyhow!("cancelled")));
        }
        let piece = piece.map_err(|e| TaskError::Transient(e.into()))?;
        if piece.is_empty() {
            continue;
        }

        let mut buffer = piece.to_vec();
        if written + buffer.len() as u64 > task.len {
            return Err(TaskError::Transient(anyhow!(
                "MEGA sent more data than the requested range"
            )));
        }

        cipher.apply_keystream(&mut buffer);
        file.write_all(&buffer)
            .await
            .map_err(|e| TaskError::Fatal(e.into()))?;

        written += buffer.len() as u64;
        shared.add_progress(buffer.len() as u64);
    }

    // A short chunk written as if it were complete shifts every later CTR
    // offset. Roll the progress back and let the caller retry the whole task.
    if written != task.len {
        shared
            .downloaded
            .fetch_sub(written.min(shared.downloaded.load(Ordering::Relaxed)), Ordering::Relaxed);
        return Err(TaskError::Transient(anyhow!(
            "short read: got {written} of {} bytes",
            task.len
        )));
    }

    file.flush().await.map_err(|e| TaskError::Fatal(e.into()))?;
    Ok(())
}

/// Downloads one MEGA file link.
///
/// `output` is the exact path to write when given; otherwise the name comes
/// from the link's encrypted attributes and lands in `download_dir`.
pub async fn download(
    client: Client,
    url: &str,
    output: Option<String>,
    download_dir: &str,
    options: MegaOptions,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<MegaOutcome> {
    let link = parse_link(url)?;
    let key = decode_file_key(&link.key)?;

    run_download(
        client,
        &link.handle,
        None,
        &key,
        output,
        download_dir,
        options,
        cancel,
        sink,
    )
    .await
}

/// The actual download, once a handle and key exist.
///
/// Split out from [`download`] so that a node discovered inside a public
/// folder — which has no link of its own to parse — gets the same chunking,
/// resume, quota handling and MAC verification rather than a second-class
/// copy of it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_download(
    client: Client,
    handle: &str,
    folder: Option<String>,
    key: &FileKey,
    output: Option<String>,
    download_dir: &str,
    options: MegaOptions,
    cancel: CancellationToken,
    sink: Arc<dyn ProgressSink>,
) -> Result<MegaOutcome> {
    sink.state(SlotState::Inspecting);

    let api = MegaApi::new(client.clone());
    let info = api.fetch(handle, key, folder.as_deref()).await?;

    let name = info
        .name
        .clone()
        .unwrap_or_else(|| sanitize_filename(handle));

    let final_path = match output {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(download_dir).join(&name),
    };

    sink.detail(&format!("{} ({})", name, format_size(info.size)));
    sink.total(Some(info.size));

    // A finished file of exactly the right size is the common "already have
    // it" case; anything else gets overwritten via the .mctemp path.
    if !options.overwrite
        && let Ok(meta) = tokio::fs::metadata(&final_path).await
        && meta.is_file() && meta.len() == info.size
    {
        sink.finish();
        return Ok(MegaOutcome::AlreadyPresent { path: final_path });
    }

    if let Some(parent) = final_path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("Cannot create {}", parent.display()))?;
    }

    let part_path = with_suffix(&final_path, ".mctemp");
    let state_path = with_suffix(&final_path, ".mctemp.json");

    let chunks = mac_chunks(info.size);
    let target = CHUNK_SIZE_MULTI * 1024 * 1024;
    let tasks = plan_tasks(&chunks, target);

    let state = ResumeState::load(&state_path, handle, info.size, target);
    let already_done = state.done.clone();

    // Preallocate so workers can seek anywhere. Sparse on every filesystem
    // that matters, so this is not actually writing `size` bytes.
    let part = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&part_path)
        .await
        .with_context(|| format!("Cannot open {}", part_path.display()))?;
    part.set_len(info.size).await?;
    drop(part);

    let resumed_bytes: u64 = tasks
        .iter()
        .filter(|t| already_done.contains(&t.id))
        .map(|t| t.len)
        .sum();

    if resumed_bytes > 0 {
        sink.detail(&format!(
            "Resuming — {} already on disk",
            format_size(resumed_bytes)
        ));
    }

    let shared = Arc::new(Shared {
        api,
        client,
        handle: handle.to_owned(),
        folder,
        key: key.clone(),
        size: info.size,
        part_path: part_path.clone(),
        state_path: state_path.clone(),
        options: options.clone(),
        cancel: cancel.clone(),
        sink: Arc::clone(&sink),
        downloaded: AtomicU64::new(resumed_bytes),
        quota_hit: AtomicBool::new(false),
        url: Mutex::new((info.url.clone(), 0)),
        state: Mutex::new(state),
    });

    sink.state(SlotState::Downloading);
    sink.progress(resumed_bytes);

    let pending: Vec<Task> = tasks
        .into_iter()
        .filter(|t| !already_done.contains(&t.id))
        .collect();

    let workers = options.workers.clamp(1, 32);
    let mut results = stream::iter(pending.into_iter().map(|task| {
        let shared = Arc::clone(&shared);
        async move { run_task(shared, task).await }
    }))
    .buffer_unordered(workers);

    while let Some(result) = results.next().await {
        result?;
    }

    if cancel.is_cancelled() {
        sink.finish();
        return Ok(MegaOutcome::Cancelled { path: part_path });
    }

    sink.state(SlotState::Finishing);

    let on_disk = tokio::fs::metadata(&part_path).await?.len();
    if on_disk != shared.size {
        bail!(
            "MEGA download finished at {} bytes but should be {}",
            on_disk,
            shared.size
        );
    }

    if options.verify_mac {
        sink.detail("Checking file integrity…");
        sink.progress(0);
        let ok = verify_file(&part_path, key, shared.size, &cancel, &sink).await?;
        if cancel.is_cancelled() {
            sink.finish();
            return Ok(MegaOutcome::Cancelled { path: part_path });
        }
        if !ok {
            // Keep the resume sidecar off the corrupt file so a rerun starts
            // clean rather than "resuming" bytes we know are wrong.
            let _ = tokio::fs::remove_file(&state_path).await;
            let _ = tokio::fs::remove_file(&part_path).await;
            bail!(
                "MEGA integrity check FAILED for {} — the file was damaged in transit and has been deleted",
                name
            );
        }
        sink.detail("Integrity check passed");
    }

    tokio::fs::rename(&part_path, &final_path)
        .await
        .with_context(|| format!("Cannot move {} into place", part_path.display()))?;
    let _ = tokio::fs::remove_file(&state_path).await;

    sink.finish();
    Ok(MegaOutcome::Completed {
        path: final_path,
        bytes: shared.size,
    })
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_mega_hosts_only() {
        assert!(is_mega_url("https://mega.nz/file/abc#key"));
        assert!(is_mega_url("https://mega.co.nz/#!abc!key"));
        assert!(is_mega_url("HTTPS://MEGA.NZ/file/abc#key"));
        assert!(!is_mega_url("https://example.com/mega.nz/file"));
        // The classic lookalike: a host that merely ends with the string.
        assert!(!is_mega_url("https://notmega.nz/file/abc#key"));
        assert!(!is_mega_url("https://mega.nz.evil.com/file/abc#key"));
    }

    #[test]
    fn parses_both_link_layouts() {
        let modern = parse_link("https://mega.nz/file/AbCdEfGh#thekey").unwrap();
        assert_eq!(modern.handle, "AbCdEfGh");
        assert_eq!(modern.key, "thekey");

        let legacy = parse_link("https://mega.nz/#!AbCdEfGh!thekey").unwrap();
        assert_eq!(legacy, modern);
    }

    #[test]
    fn rejects_links_we_cannot_handle() {
        assert!(parse_link("https://mega.nz/file/AbCdEfGh").is_err());
        assert!(parse_link("https://mega.nz/folder/AbCdEfGh#key").is_err());
        assert!(parse_link("https://example.com/file/a#b").is_err());
    }

    #[test]
    fn file_key_is_the_xor_of_both_halves() {
        let mut raw = [0u8; 32];
        for (i, byte) in raw.iter_mut().enumerate() {
            *byte = i as u8;
        }
        let encoded = URL_SAFE_NO_PAD.encode(raw);
        let key = decode_file_key(&encoded).unwrap();

        for i in 0..16 {
            assert_eq!(key.aes[i], (i as u8) ^ ((i + 16) as u8));
        }
        assert_eq!(key.nonce, [16, 17, 18, 19, 20, 21, 22, 23]);
        assert_eq!(key.meta_mac, [24, 25, 26, 27, 28, 29, 30, 31]);
    }

    #[test]
    fn key_decoding_tolerates_mangled_base64() {
        let raw = [7u8; 32];
        let url_safe = URL_SAFE_NO_PAD.encode(raw);
        let standard = url_safe.replace('-', "+").replace('_', "/") + "==";
        assert_eq!(
            decode_file_key(&url_safe).unwrap(),
            decode_file_key(&standard).unwrap()
        );
    }

    #[test]
    fn short_keys_are_rejected_with_a_hint() {
        let err = decode_file_key(&URL_SAFE_NO_PAD.encode([1u8; 22]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("32 bytes"), "{err}");
    }

    /// The ladder is fixed by MEGA. If this test changes, every download
    /// starts failing verification.
    #[test]
    fn mac_ladder_ramps_then_settles_at_one_mib() {
        let chunks = mac_chunks(10 * 1024 * 1024);
        assert_eq!(chunks[0], (0, 128 * 1024));
        assert_eq!(chunks[1], (128 * 1024, 256 * 1024));
        assert_eq!(chunks[6].1, 896 * 1024);
        // 128K * (1+2+...+7) == 3584K, where the 1 MiB run begins.
        assert_eq!(chunks[7].0, 3584 * 1024);
        assert_eq!(chunks[7].1, 1024 * 1024);

        let total: u64 = chunks.iter().map(|c| c.1).sum();
        assert_eq!(total, 10 * 1024 * 1024);
    }

    #[test]
    fn mac_ladder_covers_odd_sizes_exactly() {
        for size in [0u64, 1, 127, 128 * 1024, 3584 * 1024 + 1, 9_999_999] {
            let chunks = mac_chunks(size);
            assert_eq!(chunks.iter().map(|c| c.1).sum::<u64>(), size);
            // Contiguous, no gaps or overlaps.
            let mut expected = 0;
            for (offset, len) in &chunks {
                assert_eq!(*offset, expected);
                expected += len;
            }
        }
        assert!(mac_chunks(0).is_empty());
    }

    #[test]
    fn tasks_tile_the_file_without_splitting_mac_chunks() {
        let size = 200 * 1024 * 1024;
        let chunks = mac_chunks(size);
        let tasks = plan_tasks(&chunks, CHUNK_SIZE_MULTI * 1024 * 1024);

        assert_eq!(tasks[0].offset, 0);
        assert_eq!(tasks.iter().map(|t| t.len).sum::<u64>(), size);

        let boundaries: BTreeSet<u64> = chunks.iter().map(|c| c.0).collect();
        let mut cursor = 0;
        for task in &tasks {
            assert_eq!(task.offset, cursor);
            assert!(boundaries.contains(&task.offset));
            assert!(task.len <= CHUNK_SIZE_MULTI * 1024 * 1024);
            cursor += task.len;
        }
        assert_eq!(cursor, size);
    }

    /// A target smaller than one MAC chunk must still make progress rather
    /// than emitting zero-length tasks forever.
    #[test]
    fn tiny_targets_still_terminate() {
        let chunks = mac_chunks(5 * 1024 * 1024);
        let tasks = plan_tasks(&chunks, 1);
        assert_eq!(tasks.len(), chunks.len());
        assert!(tasks.iter().all(|t| t.len > 0));
    }

    #[test]
    fn ctr_counter_is_the_block_index() {
        let nonce = [1u8, 2, 3, 4, 5, 6, 7, 8];
        assert_eq!(&ctr_iv(&nonce, 0)[8..], &[0u8; 8]);
        assert_eq!(ctr_iv(&nonce, 16)[15], 1);
        assert_eq!(ctr_iv(&nonce, 1024 * 1024)[13..], [1, 0, 0]);
        assert_eq!(&ctr_iv(&nonce, 4096)[..8], &nonce);
    }

    /// NIST SP 800-38A, F.5.1 (CTR-AES128.Encrypt). These are the reference
    /// vectors for exactly the construction the `ctr` crate used to provide,
    /// so passing them is what makes replacing it safe.
    #[test]
    fn ctr_matches_the_nist_vectors() {
        let key: [u8; 16] = [
            0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf,
            0x4f, 0x3c,
        ];
        let iv: [u8; 16] = [
            0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd,
            0xfe, 0xff,
        ];

        let plaintext: [u8; 32] = [
            0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96, 0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93,
            0x17, 0x2a, 0xae, 0x2d, 0x8a, 0x57, 0x1e, 0x03, 0xac, 0x9c, 0x9e, 0xb7, 0x6f, 0xac,
            0x45, 0xaf, 0x8e, 0x51,
        ];
        let expected: [u8; 32] = [
            0x87, 0x4d, 0x61, 0x91, 0xb6, 0x20, 0xe3, 0x26, 0x1b, 0xef, 0x68, 0x64, 0x99, 0x0d,
            0xb6, 0xce, 0x98, 0x06, 0xf6, 0x6b, 0x79, 0x70, 0xfd, 0xff, 0x86, 0x17, 0x18, 0x7b,
            0xb9, 0xff, 0xfd, 0xff,
        ];

        let mut buffer = plaintext;
        Aes128Ctr::new(&key, &iv).apply_keystream(&mut buffer);
        assert_eq!(buffer, expected);

        // CTR is its own inverse.
        Aes128Ctr::new(&key, &iv).apply_keystream(&mut buffer);
        assert_eq!(buffer, plaintext);
    }

    /// The counter is 128-bit big-endian, so incrementing past 0xff must carry
    /// into the next byte rather than wrapping in place.
    #[test]
    fn ctr_counter_carries_across_byte_boundaries() {
        let key = [1u8; 16];
        let mut iv = [0u8; 16];
        iv[15] = 0xff;

        let mut cipher = Aes128Ctr::new(&key, &iv);
        cipher.refill();
        // 0x...ff + 1 == 0x...0100, not 0x...00.
        assert_eq!(cipher.counter[15], 0x00);
        assert_eq!(cipher.counter[14], 0x01);

        // And the same one block further on.
        cipher.refill();
        assert_eq!(cipher.counter[15], 0x01);
        assert_eq!(cipher.counter[14], 0x01);
    }

    /// The download feeds this whatever the socket returned, so the keystream
    /// has to be indifferent to how the data is sliced.
    #[test]
    fn ctr_is_independent_of_write_boundaries() {
        let key = [17u8; 16];
        let iv = [4u8; 16];
        let data: Vec<u8> = (0..1000).map(|i| (i % 251) as u8).collect();

        let mut all_at_once = data.clone();
        Aes128Ctr::new(&key, &iv).apply_keystream(&mut all_at_once);

        let mut dribbled = data.clone();
        let mut cipher = Aes128Ctr::new(&key, &iv);
        for piece in dribbled.chunks_mut(7) {
            cipher.apply_keystream(piece);
        }

        assert_eq!(all_at_once, dribbled);
    }

    /// Decrypting a chunk in isolation must match decrypting the whole stream,
    /// which is the property the entire parallel design rests on.
    #[test]
    fn parallel_offsets_decrypt_identically() {
        let key = FileKey {
            aes: [9u8; 16],
            nonce: [3u8; 8],
            meta_mac: [0u8; 8],
        };
        let plaintext: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();

        let mut whole = plaintext.clone();
        Aes128Ctr::new(&key.aes, &ctr_iv(&key.nonce, 0)).apply_keystream(&mut whole);

        let mut second_half = whole[2048..].to_vec();
        Aes128Ctr::new(&key.aes, &ctr_iv(&key.nonce, 2048)).apply_keystream(&mut second_half);

        assert_eq!(second_half, plaintext[2048..]);
    }

    #[test]
    fn chunk_mac_pads_the_final_block_with_zeroes() {
        let key = FileKey {
            aes: [4u8; 16],
            nonce: [8u8; 8],
            meta_mac: [0u8; 8],
        };

        let mut short = ChunkMac::new(&key);
        short.update(&[1, 2, 3]);

        let mut padded = ChunkMac::new(&key);
        let mut block = vec![0u8; 16];
        block[0] = 1;
        block[1] = 2;
        block[2] = 3;
        padded.update(&block);

        assert_eq!(short.finish(), padded.finish());
    }

    #[test]
    fn mac_is_sensitive_to_a_single_flipped_bit() {
        let key = FileKey {
            aes: [2u8; 16],
            nonce: [5u8; 8],
            meta_mac: [0u8; 8],
        };
        let data = vec![42u8; 100_000];

        let mac_of = |bytes: &[u8]| {
            let mut mac = ChunkMac::new(&key);
            mac.update(bytes);
            combine_macs(&key, &[mac.finish()])
        };

        let mut tampered = data.clone();
        tampered[50_000] ^= 0x01;
        assert_ne!(mac_of(&data), mac_of(&tampered));
    }

    /// Feeding the MAC in arbitrary pieces must not change the result — the
    /// download hands it whatever the socket happened to return.
    #[test]
    fn mac_is_independent_of_read_boundaries() {
        let key = FileKey {
            aes: [11u8; 16],
            nonce: [13u8; 8],
            meta_mac: [0u8; 8],
        };
        let data: Vec<u8> = (0..5000).map(|i| (i % 97) as u8).collect();

        let mut all_at_once = ChunkMac::new(&key);
        all_at_once.update(&data);

        let mut dribbled = ChunkMac::new(&key);
        for piece in data.chunks(7) {
            dribbled.update(piece);
        }

        assert_eq!(all_at_once.finish(), dribbled.finish());
    }

    #[test]
    fn filenames_cannot_escape_the_download_directory() {
        assert_eq!(sanitize_filename("../../.bashrc"), ".bashrc");
        assert_eq!(sanitize_filename("/etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("C:\\Windows\\evil.exe"), "evil.exe");
        assert_eq!(sanitize_filename("holiday.mkv"), "holiday.mkv");
        assert_eq!(sanitize_filename("  "), "mega-download");
        assert_eq!(sanitize_filename(".."), "mega-download");
        assert!(!sanitize_filename("bad\nname").contains('\n'));
    }

    /// Dotfiles are real filenames. Stripping the leading dot would write a
    /// different file than the one in the link.
    #[test]
    fn dotfiles_keep_their_leading_dot() {
        assert_eq!(sanitize_filename(".bashrc"), ".bashrc");
        assert_eq!(sanitize_filename(".config.tar.gz"), ".config.tar.gz");
        // Names that are nothing but dots are traversal, not dotfiles.
        assert_eq!(sanitize_filename("."), "mega-download");
        assert_eq!(sanitize_filename("..."), "mega-download");
    }

    /// ECB is only ever used to unwrap keys, so the property that matters is
    /// that it round-trips whole blocks independently.
    #[test]
    fn ecb_unwraps_whole_blocks() {
        let key = [3u8; 16];
        let cipher = Aes128::new(&key.into());
        let plain: Vec<u8> = (0..32u8).collect();

        let mut wrapped = plain.clone();
        for block_start in (0..32).step_by(16) {
            let mut block = [0u8; 16];
            block.copy_from_slice(&wrapped[block_start..block_start + 16]);
            encrypt_block(&cipher, &mut block);
            wrapped[block_start..block_start + 16].copy_from_slice(&block);
        }

        assert_eq!(ecb_decrypt(&key, &wrapped), plain);
        // Trailing bytes that do not fill a block are dropped, not padded.
        assert_eq!(ecb_decrypt(&key, &wrapped[..20]).len(), 16);
    }

    #[test]
    fn fatal_codes_are_not_retried() {
        assert!(is_fatal_api_error(-9));
        assert!(is_fatal_api_error(-16));
        // Quota and "temporarily unavailable" both clear on their own.
        assert!(!is_fatal_api_error(-17));
        assert!(!is_fatal_api_error(-18));
        assert!(!is_fatal_api_error(-3));
    }

    #[test]
    fn api_errors_explain_themselves() {
        assert!(api_error_message(-9).contains("not found"));
        assert!(api_error_message(-17).contains("quota"));
        assert!(api_error_message(-42).contains("-42"));
    }

    #[test]
    fn backoff_grows_then_caps() {
        assert!(backoff_delay(1) < backoff_delay(3));
        assert!(backoff_delay(50) <= MAX_BACKOFF);
    }

    #[test]
    fn temp_paths_sit_beside_the_target() {
        let path = PathBuf::from("/tmp/dl/movie.mkv");
        assert_eq!(
            with_suffix(&path, ".mctemp"),
            PathBuf::from("/tmp/dl/movie.mkv.mctemp")
        );
    }

    /// Resume state from a different file (same path, new link) must be
    /// discarded rather than used to skip chunks of the new file.
    #[test]
    fn resume_state_is_bound_to_handle_and_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let mut state = ResumeState::fresh("handleA", 1000, 64);
        state.done.insert(3);
        state.save(&path);

        let same = ResumeState::load(&path, "handleA", 1000, 64);
        assert!(same.done.contains(&3));

        assert!(ResumeState::load(&path, "handleB", 1000, 64).done.is_empty());
        assert!(ResumeState::load(&path, "handleA", 2000, 64).done.is_empty());
        // A different task size means the ids mean something else entirely.
        assert!(ResumeState::load(&path, "handleA", 1000, 32).done.is_empty());
    }

    #[tokio::test]
    async fn verify_file_accepts_good_bytes_and_rejects_bad() {
        let key = FileKey {
            aes: [21u8; 16],
            nonce: [6u8; 8],
            meta_mac: [0u8; 8],
        };
        let data: Vec<u8> = (0..300_000).map(|i| (i % 211) as u8).collect();

        let macs: Vec<[u8; 16]> = mac_chunks(data.len() as u64)
            .into_iter()
            .map(|(offset, len)| {
                let mut mac = ChunkMac::new(&key);
                mac.update(&data[offset as usize..(offset + len) as usize]);
                mac.finish()
            })
            .collect();

        let key = FileKey {
            meta_mac: combine_macs(&key, &macs),
            ..key
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload.bin");
        tokio::fs::write(&path, &data).await.unwrap();

        let sink = crate::ui::silent();
        let cancel = CancellationToken::new();
        assert!(
            verify_file(&path, &key, data.len() as u64, &cancel, &sink)
                .await
                .unwrap()
        );

        let mut corrupt = data.clone();
        corrupt[123_456] ^= 0xff;
        tokio::fs::write(&path, &corrupt).await.unwrap();
        assert!(
            !verify_file(&path, &key, corrupt.len() as u64, &cancel, &sink)
                .await
                .unwrap()
        );
    }
}
