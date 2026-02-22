use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;

static FORCE_EXIT: AtomicBool = AtomicBool::new(false);

pub async fn wait_for_ctrl_c(cancel: CancellationToken) {
    let first = tokio::signal::ctrl_c().await;
    if first.is_err() {
        return;
    }

    eprintln!("\n  ⚠ Cancelling download... (press Ctrl+C again to force quit)");
    cancel.cancel();

    FORCE_EXIT.store(true, Ordering::SeqCst);

    let second = tokio::signal::ctrl_c().await;
    if second.is_ok() {
        eprintln!("\n  ✖ Force quit.");
        std::process::exit(1);
    }
}

pub fn spawn_signal_handler(cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(wait_for_ctrl_c(cancel))
}
