use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;

static FORCE_EXIT: AtomicBool = AtomicBool::new(false);

// FIX 6: Wait for either SIGINT (Ctrl+C) or SIGTERM
async fn wait_for_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut sigterm) => {
            tokio::select! {
                result = tokio::signal::ctrl_c() => result,
                _ = sigterm.recv() => Ok(()),
            }
        }
        Err(_) => {
            // SIGTERM registration failed — fall back to SIGINT only
            tokio::signal::ctrl_c().await
        }
    }
}

pub async fn wait_for_ctrl_c(cancel: CancellationToken) {
    // FIX 6: First signal — SIGINT or SIGTERM
    let first = wait_for_signal().await;
    if first.is_err() {
        return;
    }

    eprintln!("\n  ⚠ Cancelling download... (press Ctrl+C again to force quit)");
    cancel.cancel();

    FORCE_EXIT.store(true, Ordering::SeqCst);

    // FIX 6: Second signal — SIGINT or SIGTERM
    let second = wait_for_signal().await;
    if second.is_ok() {
        eprintln!("\n  ✖ Force quit.");
        std::process::exit(1);
    }
}

pub fn spawn_signal_handler(cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(wait_for_ctrl_c(cancel))
}
