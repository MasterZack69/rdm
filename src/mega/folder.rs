//! MEGA folder links.
//!
//! A folder link (`mega.nz/folder/<handle>#<key>`) is a different animal from
//! a file link. The fragment is a **16-byte share key**, not a 32-byte file
//! key, and the handle names a *node tree* rather than a blob. Getting a file
//! out of one takes an extra round trip:
//!
//! 1. `POST /cs?n=<folder>` with `{"a":"f","c":1,"r":1}` returns every node in
//!    the share — no auth, no pagination, the whole tree in one response.
//! 2. Each node carries `k` = `"<share>:<wrapped key>"`. Unwrapping is
//!    AES-128-ECB under the share key. Folder nodes unwrap to 16 bytes; file
//!    nodes unwrap to the same 32 bytes a file link's fragment carries, which
//!    is why everything below this module is shared with the file path.
//! 3. Each node's `a` blob decrypts with *that node's* key to give its name.
//! 4. From there it is an ordinary download, except that the temp-URL request
//!    is scoped to the share (`?n=<folder>`, and `n` rather than `p` in the
//!    command).
//!
//! ## Picking the right key
//!
//! `k` can hold several wrappings separated by `/`, one per share the node is
//! reachable through. Only one of them is ours. The others do not *fail* to
//! decrypt — AES-ECB happily turns them into 32 bytes of noise that passes
//! every structural check there is. Choosing wrong is therefore silent: the
//! download runs to completion and only dies at the CBC-MAC, having spent the
//! bandwidth.
//!
//! So the attribute blob is used as an oracle. It decrypts to plaintext
//! beginning with the literal `MEGA` only under the correct key, which makes
//! "can we read this node's name?" a reliable test of "is this the right
//! key?", and a free one, since the name is needed anyway.
//!
//! ## Where files land
//!
//! Paths are relative to the share root, and the root's own name is not part
//! of them. A share holding `folder/one.jpg` and `two.jpg` unpacks into the
//! destination directory as exactly that, so `-o` and the configured download
//! directory behave identically instead of one of them nesting an extra level
//! named after the share.
//!
//! ## Path safety
//!
//! Directory structure is reconstructed by walking `p` (parent) pointers, so
//! **every** component is attacker-controlled, not just the filename. A folder
//! inside a share can be called `..` exactly as easily as a file can. Names
//! are sanitised as they come out of the attribute blob and again when the
//! path is joined, and the parent walk is depth-capped so a malformed tree
//! with a parent cycle cannot spin forever.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::{
    FileKey, MegaApi, MegaOptions, MegaOutcome, b64_decode, decrypt_attributes, ecb_decrypt,
    file_key_from_bytes, is_mega_url, link_path, run_download, sanitize_filename,
};
use crate::ui::ProgressSink;

/// A parent chain longer than this is a malformed or hostile tree.
const MAX_DEPTH: usize = 64;

// ── Link parsing ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolderLink {
    /// The share handle. This is what `?n=` is set to.
    pub handle: String,
    /// Base64 share key. Never sent to MEGA.
    pub key: String,
    /// A specific node the link points at, when the URL names one.
    pub node: Option<String>,
}

/// Is this a folder share rather than a single file?
pub fn is_folder_link(url: &str) -> bool {
    if !is_mega_url(url) {
        return false;
    }
    let path = link_path(url.trim());
    path.starts_with("folder/") || path.starts_with("#F!")
}

/// Splits a folder link into share handle, share key and optional node.
///
/// Three layouts show up in the wild:
///
/// ```text
/// https://mega.nz/folder/<handle>#<key>
/// https://mega.nz/folder/<handle>#<key>/file/<node>
/// https://mega.nz/#F!<handle>!<key>
/// ```
pub fn parse_folder_link(url: &str) -> Result<FolderLink> {
    let url = url.trim();
    if !is_mega_url(url) {
        bail!("Not a mega.nz link: {url}");
    }

    let path = link_path(url);

    let (handle, tail) = if let Some(rest) = path.strip_prefix("folder/") {
        rest.split_once('#')
            .ok_or_else(|| anyhow!("MEGA folder link has no share key after '#'"))?
    } else if let Some(rest) = path.strip_prefix("#F!") {
        rest.split_once('!')
            .ok_or_else(|| anyhow!("MEGA folder link has no share key after '!'"))?
    } else {
        bail!("Not a MEGA folder link: {url}");
    };

    // Anything after the key selects a node inside the share.
    let mut parts = tail.split('/');
    let key = parts.next().unwrap_or("").trim();
    let node = match (parts.next(), parts.next()) {
        (Some("file"), Some(node)) | (Some("folder"), Some(node)) => {
            Some(node.trim().to_string())
        }
        _ => None,
    };

    let handle = handle.trim().to_string();
    if handle.is_empty() || key.is_empty() {
        bail!("MEGA folder link is missing its handle or key");
    }

    Ok(FolderLink {
        handle,
        key: key.to_string(),
        node: node.filter(|n| !n.is_empty()),
    })
}

/// Decodes the share key.
///
/// Folder keys are 16 bytes, where file keys are 32. Feeding one to the file
/// key decoder is the single most likely mix-up here, so the length is
/// checked explicitly.
pub fn decode_folder_key(key_b64: &str) -> Result<[u8; 16]> {
    let raw = b64_decode(key_b64)?;
    if raw.len() < 16 {
        bail!(
            "MEGA folder key must decode to 16 bytes, got {}",
            raw.len()
        );
    }
    let mut key = [0u8; 16];
    key.copy_from_slice(&raw[..16]);
    Ok(key)
}

// ── Node tree ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RawNode {
    /// Node handle.
    #[serde(default)]
    h: Option<String>,
    /// Parent handle.
    #[serde(default)]
    p: Option<String>,
    /// 0 = file, 1 = folder, 2 = share root.
    #[serde(default)]
    t: Option<i64>,
    /// Size, files only.
    #[serde(default)]
    s: Option<u64>,
    /// Encrypted attributes.
    #[serde(default)]
    a: Option<String>,
    /// Wrapped node key, `"<share>:<base64>"`.
    #[serde(default)]
    k: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TreeResponse {
    #[serde(default)]
    f: Vec<RawNode>,
}

/// One downloadable file inside a share.
#[derive(Debug, Clone)]
pub struct Entry {
    /// Node handle, used as the `n` of the temp-URL request.
    pub handle: String,
    /// Sanitised path components, ending in the filename.
    pub path: Vec<String>,
    pub size: u64,
    pub key: FileKey,
    /// Handles of every folder above this file, root-first.
    pub ancestors: Vec<String>,
}

impl Entry {
    /// Relative path as text, for logs and progress lines.
    pub fn display_path(&self) -> String {
        self.path.join("/")
    }
}

/// The contents of a share.
#[derive(Debug, Clone)]
pub struct Listing {
    /// Name of the share root. Informational only — it is deliberately not
    /// part of any destination path.
    pub root_name: String,
    pub entries: Vec<Entry>,
    /// Handles of file nodes this share key cannot open.
    pub undecryptable: Vec<String>,
}

impl Listing {
    pub fn total_bytes(&self) -> u64 {
        self.entries.iter().map(|e| e.size).sum()
    }
}

/// Every plausible unwrapping of a node key, best candidate first.
///
/// `k` lists one wrapping per share the node is reachable through. The part
/// before `:` names that share, so when it matches ours that candidate is
/// tried first — but it is only a hint, not a guarantee, and nodes do turn up
/// with no prefix at all. Everything is kept and the caller decides which one
/// actually works.
fn unwrap_candidates(share: &[u8; 16], k: &str, share_handle: &str) -> Vec<Vec<u8>> {
    let mut preferred = Vec::new();
    let mut rest = Vec::new();

    for part in k.split('/') {
        let (owner, encoded) = match part.split_once(':') {
            Some((owner, encoded)) => (Some(owner), encoded),
            None => (None, part),
        };

        let Ok(raw) = b64_decode(encoded) else {
            continue;
        };
        if raw.len() < 16 {
            continue;
        }

        let decrypted = ecb_decrypt(share, &raw);
        if decrypted.is_empty() {
            continue;
        }

        if owner == Some(share_handle) {
            preferred.push(decrypted);
        } else {
            rest.push(decrypted);
        }
    }

    preferred.extend(rest);
    preferred
}

/// Picks the file key that actually opens this node.
///
/// The attribute blob is the oracle: it only decrypts to a `MEGA`-prefixed
/// payload under the right key. Without this check a wrong candidate is
/// indistinguishable from a right one until the MAC fails at 100%.
///
/// A node with no attributes at all has nothing to test against, so the first
/// candidate is taken on faith — the MAC remains the backstop.
fn choose_file_key(candidates: &[Vec<u8>], attributes: Option<&str>) -> Option<(FileKey, String)> {
    let mut fallback = None;

    for raw in candidates {
        let Ok(key) = file_key_from_bytes(raw) else {
            continue;
        };

        match attributes {
            Some(at) => {
                if let Some(name) = decrypt_attributes(at, &key.aes) {
                    return Some((key, name));
                }
            }
            None => {
                if fallback.is_none() {
                    fallback = Some(key);
                }
            }
        }
    }

    fallback.map(|key| (key, String::new()))
}

/// Joins sanitised components under `base`.
///
/// Sanitising again here is deliberate belt-and-braces: this is the last point
/// before a MEGA-supplied name becomes a real filesystem path.
fn join_path(base: &Path, components: &[String]) -> PathBuf {
    let mut path = base.to_path_buf();
    for part in components {
        path.push(sanitize_filename(part));
    }
    path
}

/// Fetches and decrypts the node tree.
pub async fn list_folder(client: &Client, link: &FolderLink) -> Result<Listing> {
    let share = decode_folder_key(&link.key)?;
    let api = MegaApi::new(client.clone());

    let raw = api
        .call(json!({ "a": "f", "c": 1, "r": 1 }), Some(&link.handle))
        .await
        .context("Cannot list the MEGA folder")?;

    let tree: TreeResponse =
        serde_json::from_value(raw).context("Unexpected MEGA folder listing shape")?;

    if tree.f.is_empty() {
        bail!("MEGA returned an empty folder listing — the share may have been removed");
    }

    let mut names: HashMap<String, String> = HashMap::new();
    let mut parents: HashMap<String, String> = HashMap::new();
    let mut files: Vec<(String, u64, FileKey)> = Vec::new();
    let mut undecryptable: Vec<String> = Vec::new();

    for node in &tree.f {
        let Some(handle) = node.h.clone() else {
            continue;
        };

        if let Some(parent) = node.p.clone()
            && !parent.is_empty()
        {
            parents.insert(handle.clone(), parent);
        }

        let candidates = node
            .k
            .as_deref()
            .map(|k| unwrap_candidates(&share, k, &link.handle))
            .unwrap_or_default();

        if node.t.unwrap_or(0) == 0 {
            // A file whose key we cannot establish is left alone rather than
            // downloaded into a MAC failure. It is usually a node shared
            // through a key we were not given.
            let Some((key, name)) = choose_file_key(&candidates, node.a.as_deref()) else {
                undecryptable.push(handle);
                continue;
            };

            let name = if name.is_empty() {
                sanitize_filename(&handle)
            } else {
                name
            };

            names.insert(handle.clone(), name);
            files.push((handle, node.s.unwrap_or(0), key));
        } else {
            // The share root's own key *is* the share key, and it is often
            // absent from `k` entirely. Folder keys are 16 bytes, and the same
            // attribute-blob oracle picks between candidates.
            let mut chosen: Option<(([u8; 16]), String)> = None;

            for raw in &candidates {
                if raw.len() < 16 {
                    continue;
                }
                let mut aes = [0u8; 16];
                aes.copy_from_slice(&raw[..16]);

                if let Some(at) = node.a.as_deref()
                    && let Some(name) = decrypt_attributes(at, &aes)
                {
                    chosen = Some((aes, name));
                    break;
                }
                if chosen.is_none() {
                    chosen = Some((aes, String::new()));
                }
            }

            let (aes, name) = chosen.unwrap_or((share, String::new()));
            let name = if name.is_empty() {
                node.a
                    .as_deref()
                    .and_then(|at| decrypt_attributes(at, &aes))
                    .unwrap_or_else(|| sanitize_filename(&handle))
            } else {
                name
            };

            names.insert(handle, name);
        }
    }

    let root = tree
        .f
        .iter()
        .find(|n| n.t == Some(2))
        .and_then(|n| n.h.clone());

    let root_name = root
        .as_ref()
        .and_then(|h| names.get(h).cloned())
        .unwrap_or_else(|| sanitize_filename(&link.handle));

    let mut entries = Vec::with_capacity(files.len());
    for (handle, size, key) in files {
        let filename = names
            .get(&handle)
            .cloned()
            .unwrap_or_else(|| sanitize_filename(&handle));

        let mut folders: Vec<String> = Vec::new();
        let mut ancestors: Vec<String> = Vec::new();
        let mut cursor = parents.get(&handle).cloned();

        while let Some(current) = cursor {
            if Some(&current) == root.as_ref() || folders.len() >= MAX_DEPTH {
                break;
            }
            ancestors.push(current.clone());
            if let Some(name) = names.get(&current) {
                folders.push(name.clone());
            }
            cursor = parents.get(&current).cloned();
        }

        folders.reverse();
        ancestors.reverse();
        folders.push(filename);

        entries.push(Entry {
            handle,
            path: folders,
            size,
            key,
            ancestors,
        });
    }

    entries.sort_by(|a, b| a.path.cmp(&b.path));

    if entries.is_empty() {
        bail!(
            "No downloadable files in this MEGA folder ({} node(s) could not be decrypted with this key)",
            undecryptable.len()
        );
    }

    Ok(Listing {
        root_name,
        entries,
        undecryptable,
    })
}

// ── Download ──────────────────────────────────────────────────────

/// What became of a folder download.
#[derive(Debug, Clone, Default)]
pub struct FolderSummary {
    /// Directory the share was unpacked into.
    pub root: PathBuf,
    pub total: usize,
    pub completed: usize,
    pub skipped: usize,
    pub bytes: u64,
    /// `(relative path, reason)` for each file that did not make it.
    pub failed: Vec<(String, String)>,
    pub cancelled: bool,
}

/// Downloads every file in a folder share, one after another.
///
/// Files are written to `output` when given and the configured download
/// directory otherwise, each at its path relative to the share root. The
/// share's own name is not inserted: a link is a handle to *contents*, and
/// wrapping those in a directory the user did not ask for is doubly unhelpful
/// when the share root has no readable name and that directory ends up called
/// something like `s6lVFYbI`.
///
/// Sequential on purpose: each file already spreads itself across several
/// chunk workers sharing one per-IP quota, so running whole files in parallel
/// buys throughput only until MEGA starts answering 509.
///
/// `make_sink` is called once per file, so the caller decides whether that is
/// a progress bar, a queue lane, or nothing at all.
#[allow(clippy::too_many_arguments)]
pub async fn download_folder(
    client: Client,
    url: &str,
    output: Option<String>,
    download_dir: &str,
    options: MegaOptions,
    cancel: CancellationToken,
    make_sink: &dyn Fn(&str, u64) -> Arc<dyn ProgressSink>,
) -> Result<FolderSummary> {
    let link = parse_folder_link(url)?;
    let listing = list_folder(&client, &link).await?;

    // A link that points at one node downloads that node, not the share.
    let entries: Vec<Entry> = match link.node.as_deref() {
        Some(node) => listing
            .entries
            .iter()
            .filter(|e| e.handle == node || e.ancestors.iter().any(|a| a == node))
            .cloned()
            .collect(),
        None => listing.entries.clone(),
    };

    if entries.is_empty() {
        bail!("That node is not in this MEGA folder, or holds no files");
    }

    let base = match output {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(download_dir),
    };

    let mut summary = FolderSummary {
        root: base.clone(),
        total: entries.len() + listing.undecryptable.len(),
        ..FolderSummary::default()
    };

    // Reported rather than attempted: without a working key these would just
    // download, fail the MAC and delete themselves.
    for handle in &listing.undecryptable {
        summary.failed.push((
            handle.clone(),
            "no key in this share link opens this node".to_string(),
        ));
    }

    for entry in entries {
        if cancel.is_cancelled() {
            summary.cancelled = true;
            break;
        }

        let relative = entry.display_path();
        let destination = join_path(&base, &entry.path);
        let sink = make_sink(&relative, entry.size);

        let outcome = run_download(
            client.clone(),
            &entry.handle,
            Some(link.handle.clone()),
            &entry.key,
            Some(destination.to_string_lossy().into_owned()),
            download_dir,
            options.clone(),
            cancel.clone(),
            sink,
        )
        .await;

        match outcome {
            Ok(MegaOutcome::Completed { bytes, .. }) => {
                summary.completed += 1;
                summary.bytes += bytes;
            }
            Ok(MegaOutcome::AlreadyPresent { .. }) => summary.skipped += 1,
            Ok(MegaOutcome::Cancelled { .. }) => {
                summary.cancelled = true;
                break;
            }
            // One unreadable file should not cost the user the other ninety.
            Err(err) => summary.failed.push((relative, err.to_string())),
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes::Aes128;
    use aes::cipher::generic_array::GenericArray;
    use aes::cipher::{BlockEncrypt, KeyInit};
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    /// AES-128-ECB encrypt, the inverse of what key unwrapping does.
    fn wrap(share: &[u8; 16], plain: &[u8]) -> String {
        let cipher = Aes128::new(share.into());
        let mut out = plain.to_vec();
        for start in (0..plain.len() / 16 * 16).step_by(16) {
            let mut block = GenericArray::clone_from_slice(&out[start..start + 16]);
            cipher.encrypt_block(&mut block);
            out[start..start + 16].copy_from_slice(&block);
        }
        URL_SAFE_NO_PAD.encode(&out)
    }

    /// Builds an `a` blob the way MEGA does: AES-128-CBC, zero IV, over
    /// `MEGA{"n":"..."}` zero-padded to a block boundary.
    fn seal_attributes(aes: &[u8; 16], name: &str) -> String {
        let mut plain = format!("MEGA{{\"n\":\"{name}\"}}").into_bytes();
        while plain.len() % 16 != 0 {
            plain.push(0);
        }

        let cipher = Aes128::new(aes.into());
        let mut previous = [0u8; 16];
        let mut out = Vec::with_capacity(plain.len());

        for chunk in plain.chunks(16) {
            let mut block = [0u8; 16];
            for i in 0..16 {
                block[i] = chunk[i] ^ previous[i];
            }
            let mut ga = GenericArray::clone_from_slice(&block);
            cipher.encrypt_block(&mut ga);
            previous.copy_from_slice(&ga);
            out.extend_from_slice(&ga);
        }

        URL_SAFE_NO_PAD.encode(&out)
    }

    #[test]
    fn folder_links_are_told_apart_from_file_links() {
        assert!(is_folder_link("https://mega.nz/folder/s6lVFYbI#XKN8d1JVkhLYqpd9"));
        assert!(is_folder_link("https://mega.nz/#F!s6lVFYbI!XKN8d1JVkhLYqpd9"));
        assert!(!is_folder_link("https://mega.nz/file/AbCdEfGh#key"));
        assert!(!is_folder_link("https://example.com/folder/x#y"));
    }

    #[test]
    fn parses_every_folder_layout() {
        let plain = parse_folder_link("https://mega.nz/folder/s6lVFYbI#XKN8d1JV").unwrap();
        assert_eq!(plain.handle, "s6lVFYbI");
        assert_eq!(plain.key, "XKN8d1JV");
        assert_eq!(plain.node, None);

        let legacy = parse_folder_link("https://mega.nz/#F!s6lVFYbI!XKN8d1JV").unwrap();
        assert_eq!(legacy, plain);

        let one_file =
            parse_folder_link("https://mega.nz/folder/s6lVFYbI#XKN8d1JV/file/QrStUvWx").unwrap();
        assert_eq!(one_file.key, "XKN8d1JV");
        assert_eq!(one_file.node.as_deref(), Some("QrStUvWx"));

        let subfolder =
            parse_folder_link("https://mega.nz/folder/s6lVFYbI#XKN8d1JV/folder/ZzZzZzZz").unwrap();
        assert_eq!(subfolder.node.as_deref(), Some("ZzZzZzZz"));
    }

    #[test]
    fn rejects_folder_links_missing_a_key() {
        assert!(parse_folder_link("https://mega.nz/folder/s6lVFYbI").is_err());
        assert!(parse_folder_link("https://mega.nz/folder/#").is_err());
        assert!(parse_folder_link("https://mega.nz/file/AbCdEfGh#key").is_err());
    }

    /// Sixteen bytes, not thirty-two. Mixing the two up is the easiest way to
    /// get a folder "working" and producing garbage.
    #[test]
    fn folder_keys_are_sixteen_bytes() {
        let key = decode_folder_key(&URL_SAFE_NO_PAD.encode([9u8; 16])).unwrap();
        assert_eq!(key, [9u8; 16]);

        let err = decode_folder_key(&URL_SAFE_NO_PAD.encode([9u8; 8]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("16 bytes"), "{err}");
    }

    #[test]
    fn node_keys_are_unwrapped_from_the_share_key() {
        let share = [5u8; 16];
        let node_key: Vec<u8> = (0..32u8).collect();
        let k = format!("s6lVFYbI:{}", wrap(&share, &node_key));

        let candidates = unwrap_candidates(&share, &k, "s6lVFYbI");
        assert_eq!(candidates[0], node_key);

        // Some nodes omit the handle prefix entirely.
        let bare = wrap(&share, &node_key);
        assert_eq!(unwrap_candidates(&share, &bare, "s6lVFYbI")[0], node_key);
    }

    /// Our own share handle is tried first when `k` lists several wrappings.
    #[test]
    fn our_share_is_preferred_among_several_wrappings() {
        let share = [5u8; 16];
        let theirs: Vec<u8> = vec![0xAA; 32];
        let ours: Vec<u8> = (0..32u8).collect();

        let k = format!(
            "OtHeRsHr:{}/s6lVFYbI:{}",
            wrap(&share, &theirs),
            wrap(&share, &ours)
        );

        let candidates = unwrap_candidates(&share, &k, "s6lVFYbI");
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], ours, "our own wrapping must be tried first");
    }

    /// The regression this module was fixed for: a wrong wrapping decrypts to
    /// 32 perfectly structural bytes, so only the attribute blob can tell the
    /// candidates apart. Picking the wrong one used to surface as a failed
    /// integrity check after the whole file had been downloaded.
    #[test]
    fn the_attribute_blob_picks_the_working_key() {
        let wrong = vec![0xAAu8; 32];
        let right: Vec<u8> = (0..32u8).collect();
        let right_key = file_key_from_bytes(&right).unwrap();
        let attributes = seal_attributes(&right_key.aes, "holiday.jpg");

        // Wrong candidate first: order must not decide the outcome.
        let candidates = vec![wrong.clone(), right.clone()];
        let (key, name) = choose_file_key(&candidates, Some(&attributes)).unwrap();

        assert_eq!(key.aes, right_key.aes);
        assert_eq!(name, "holiday.jpg");

        // With no candidate that opens the attributes, there is no answer —
        // downloading on a guess is what produced the corrupt file.
        assert!(choose_file_key(&[wrong], Some(&attributes)).is_none());
    }

    /// No attributes means no oracle, so the first structurally valid
    /// candidate is used and the MAC stays the backstop.
    #[test]
    fn a_node_without_attributes_falls_back_to_the_first_key() {
        let raw: Vec<u8> = (0..32u8).collect();
        let (key, name) = choose_file_key(&[raw.clone()], None).unwrap();
        assert_eq!(key, file_key_from_bytes(&raw).unwrap());
        assert!(name.is_empty());

        // Too short to be a file key at all.
        assert!(choose_file_key(&[vec![1u8; 16]], None).is_none());
    }

    /// The whole point of the tree walk is that folder names are as hostile as
    /// filenames.
    #[test]
    fn folder_names_cannot_climb_out_of_the_destination() {
        let base = Path::new("/tmp/dl");
        let path = join_path(
            base,
            &[
                "..".to_string(),
                "safe".to_string(),
                "../../etc".to_string(),
                "passwd".to_string(),
            ],
        );

        assert!(path.starts_with(base), "{path:?}");
        assert!(!path.to_string_lossy().contains(".."), "{path:?}");
        assert_eq!(
            path,
            PathBuf::from("/tmp/dl/mega-download/safe/etc/passwd")
        );
    }

    /// Structure inside the share is kept; the share's own name is not part of
    /// it, so a file at the top of the share lands directly in the
    /// destination.
    #[test]
    fn paths_are_relative_to_the_share_root() {
        let base = Path::new("/home/zack/test");

        assert_eq!(
            join_path(base, &["bakchodi".to_string(), "one.jpg".to_string()]),
            PathBuf::from("/home/zack/test/bakchodi/one.jpg")
        );
        assert_eq!(
            join_path(base, &["two.jpg".to_string()]),
            PathBuf::from("/home/zack/test/two.jpg")
        );
    }

    #[test]
    fn nested_paths_are_joined_in_order() {
        let path = join_path(
            Path::new("/tmp/dl"),
            &["Season 1".to_string(), "ep01.mkv".to_string()],
        );
        assert_eq!(path, PathBuf::from("/tmp/dl/Season 1/ep01.mkv"));
    }
}
