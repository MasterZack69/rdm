//! Opt-in, disposable fixture on a loopback-only SSH server. No network is
//! contacted by the default test suite; see extraInfo/sftp.md for setup.

use anyhow::{Context, Result, ensure};
use std::io::Write;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::engine::{DownloadRequest, ExistingPolicy, Outcome};
use crate::sftp::{self, SftpOptions, SftpUrl};
use crate::sftp::local::path_text;
use crate::sftp::session::with_session;
use crate::ui::{ProgressSink, Silent};

struct StopAfterChunk(CancellationToken);

impl ProgressSink for StopAfterChunk {
    fn progress(&self, bytes: u64) {
        if bytes >= 64 * 1024 { self.0.cancel(); }
    }
}

#[tokio::test]
#[ignore = "requires RDM_SFTP_TEST_URL and a disposable, trusted loopback SSH server"]
async fn download_resume_listing_queue_and_sync_against_openssh() {
    let url = std::env::var("RDM_SFTP_TEST_URL").expect("set a writable sftp://user@127.0.0.1:port/path");
    let parent = SftpUrl::parse(&url).unwrap();
    let loopback = parent.host() == "localhost"
        || parent.host().parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
    assert!(loopback, "live tests may only contact a loopback SSH server");
    let directory = tempfile::tempdir().unwrap();
    let cfg = Config {
        download_dir: path_text(directory.path()).unwrap().to_owned(),
        max_retries: 0,
        ..Config::default()
    };
    let options = SftpOptions::from_config(&cfg).unwrap();
    let fixture = parent.with_path(&parent.path().join(format!(
        "rdm-live-{}", crate::safe_file::random_token(),
    ))).unwrap();
    let payload: Vec<u8> = (0..1024 * 1024).map(|index| (index % 251) as u8).collect();
    let bytes = payload.clone();
    with_session(fixture.clone(), Arc::clone(&options.authentication), true, CancellationToken::new(),
        move |_, sftp, fixture, _| {
            sftp.mkdir(fixture.path(), 0o700)?;
            sftp.mkdir(&fixture.path().join("nested"), 0o700)?;
            for (name, data) in [
                ("payload.bin", bytes.as_slice()),
                ("empty.bin", b"".as_slice()),
                ("nested/odd %#.FLAC", b"audio".as_slice()),
            ] {
                let mut file = sftp.create(&fixture.path().join(name))?;
                file.write_all(data)?;
                file.close()?;
            }
            Ok(())
        },
    ).await.unwrap();
    let result = exercise(&cfg, &fixture, &options, &payload).await;
    // Even a failed assertion in exercise returns through fixture cleanup.
    let cleanup = with_session(fixture, Arc::clone(&options.authentication), true, CancellationToken::new(),
        |_, sftp, fixture, _| {
            for name in ["payload.bin", "empty.bin", "nested/odd %#.FLAC"] {
                sftp.unlink(&fixture.path().join(name))?;
            }
            sftp.rmdir(&fixture.path().join("nested"))?;
            sftp.rmdir(fixture.path())?;
            Ok(())
        },
    ).await;
    result.unwrap();
    cleanup.unwrap();
}

async fn exercise(cfg: &Config, fixture: &SftpUrl, options: &SftpOptions, payload: &[u8]) -> Result<()> {
    let listing = sftp::list(fixture.as_str(), options, true, CancellationToken::new()).await?
        .context("Fixture was not a directory")?;
    ensure!(listing.files.len() == 3 && listing.skipped == 0, "Wrong SFTP listing");
    let file = fixture.with_path(&fixture.path().join("payload.bin"))?;
    let output = cfg.resolve_output_path("resumed.bin");
    let request = DownloadRequest::new(file.as_str().to_owned(), Some(output.clone()), 1)
        .with_allow_private(true);
    let cancel = CancellationToken::new();
    let stopped = sftp::download(request.clone(), options.clone(), cancel.clone(), Arc::new(StopAfterChunk(cancel))).await?;
    ensure!(matches!(stopped, Outcome::Cancelled), "Transfer did not cancel");
    ensure!(!std::path::Path::new(&output).exists(), "Cancelled download was published");
    let completed = sftp::download(request, options.clone(), CancellationToken::new(), Arc::new(Silent)).await?;
    ensure!(matches!(completed, Outcome::Completed { .. }), "Resume did not complete");
    ensure!(std::fs::read(&output)? == payload, "Resumed bytes differ");

    let empty = fixture.with_path(&fixture.path().join("empty.bin"))?;
    let mut queue = crate::queue::Queue::default();
    queue.add_with_scope(empty.as_str().to_owned(), Some(String::from("queued-empty.bin")), Some(1), true);
    // Queue's item vector is intentionally private; verify the persisted
    // representation rather than widening that API for a test.
    let stored = serde_json::to_value(&queue)?;
    let item = &stored["items"][0];
    let saved_url = item["url"].as_str().context("Queue lost its SFTP URL")?;
    let saved_output = item["output"].as_str().context("Queue lost its output")?;
    ensure!(item["allow_private"].as_bool() == Some(true), "Queue lost address scope");
    let request = DownloadRequest::new(saved_url.to_owned(), Some(cfg.resolve_output_path(saved_output)), 1)
        .with_policy(ExistingPolicy::Reuse).with_allow_private(true);
    let completed = sftp::download(request, options.clone(), CancellationToken::new(), Arc::new(Silent)).await?;
    ensure!(matches!(completed, Outcome::Completed { bytes: 0, .. }), "Empty queue file failed");

    let mirror = cfg.resolve_output_path("mirror");
    std::fs::create_dir_all(&mirror)?;
    std::fs::write(std::path::Path::new(&mirror).join("payload.bin"), b"stale")?;
    std::fs::write(std::path::Path::new(&mirror).join("orphan.bin"), b"orphan")?;
    crate::sync::run(cfg, fixture.as_str(), None, 2, true, None, true, Some(mirror.clone()), CancellationToken::new()).await?;
    ensure!(std::fs::read(std::path::Path::new(&mirror).join("payload.bin"))? == payload, "Sync did not replace stale bytes");
    ensure!(!std::path::Path::new(&mirror).join("orphan.bin").exists(), "Sync left an orphan");
    ensure!(std::fs::metadata(std::path::Path::new(&mirror).join("empty.bin"))?.len() == 0, "Empty sync file failed");
    ensure!(std::fs::read(std::path::Path::new(&mirror).join("nested/odd %#.FLAC"))? == b"audio", "Encoded nested filename failed");
    Ok(())
}
