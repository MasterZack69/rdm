// it's actually not the client anymore since V0.2.10 and im too lazy to properly care about it
use std::sync::OnceLock;

use crate::config::Config;

static SHARED_CONFIG: OnceLock<Config> = OnceLock::new();

pub(super) fn shared_config() -> &'static Config {
    SHARED_CONFIG.get_or_init(Config::load)
}
