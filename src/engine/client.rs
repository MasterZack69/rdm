// it's actually not the client anymore since V0.2.10 and im too lazy to properly care about it
use std::sync::OnceLock;

use crate::config::Config;

static SHARED_CONFIG: OnceLock<Config> = OnceLock::new();

/// Installs the configuration this process already loaded.
///
/// Only the first call takes effect. `main` loads the file once, and an
/// invalid one stops the run there with a message naming the problem — which
/// is the point of this function existing: reading it again here could only
/// either duplicate that error in a place that cannot report it, or fall back
/// to defaults and silently change the retry count behind the user's back.
pub fn set_shared_config(config: Config) {
    let _ = SHARED_CONFIG.set(config);
}

pub(super) fn shared_config() -> &'static Config {
    // Defaults only when no caller installed anything — a library user, or a
    // test. Never a substitute for a config file that exists.
    SHARED_CONFIG.get_or_init(Config::default)
}
