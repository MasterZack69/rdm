mod client;
mod scope;

pub use client::{MAX_REDIRECTS, Policy, Target, probed_size, redirect_location};
pub use scope::{ScopeGuard, env_flag, parse_and_validate_url, parse_host_as_ip};
