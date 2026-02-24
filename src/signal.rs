use tokio_util::sync::CancellationToken;


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
            tokio::signal::ctrl_c().await
        }
    }
}

pub async fn wait_for_ctrl_c(cancel: CancellationToken) {
    let first = tokio::signal::ctrl_c().await;
    if first.is_err() {
        return;
    }

    eprintln!("\n  ⚠ Cancelling download...");
    cancel.cancel();

    let second = tokio::signal::ctrl_c().await;
    if second.is_ok() {
        eprintln!("\n  ✖ Force quit.");
        std::process::exit(1);
    }
}

pub fn spawn_signal_handler(cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(wait_for_ctrl_c(cancel))
}
