//! Value parsers, the limits they enforce, and `--ext` normalisation.

use std::collections::HashSet;

use super::commands::RetryTarget;

/// Upper bound for connections per file. Beyond this, servers start refusing
/// or throttling and the chunk bookkeeping stops paying for itself.
pub const MAX_CONNECTIONS: usize = 64;

/// Upper bound for how many queue items may download concurrently.
pub const MAX_PARALLEL: usize = 32;

/// Accepts only http(s) URLs.
///
/// Validating here, rather than letting an unrecognised first argument fall
/// through to "print usage and exit 1", is what lets clap report the actual
/// problem: a typo like `rdm dowload URL` gets an error naming it instead of
/// the full help text.
pub fn parse_url(value: &str) -> Result<String, String> {
    let trimmed = value.trim();

    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(trimmed.to_owned());
    }

    if trimmed.is_empty() {
        return Err("URL must not be empty".to_owned());
    }

    // Concatenated rather than formatted: the suggestion must come out as a
    // URL the user can paste, and a format-string escape here quietly wraps it
    // in literal braces instead.
    let suggestion = String::from("https://") + trimmed;
    Err(format!(
        "`{trimmed}` is not an http(s) URL \u{2014} did you mean `{suggestion}`?"
    ))
}

/// Parses `--connections`, rejecting 0 and absurd values.
pub fn parse_connections(value: &str) -> Result<usize, String> {
    parse_bounded(value, MAX_CONNECTIONS, "connections")
}

/// Parses `--parallel`, rejecting 0 and absurd values.
pub fn parse_parallel(value: &str) -> Result<usize, String> {
    parse_bounded(value, MAX_PARALLEL, "parallel downloads")
}

fn parse_bounded(value: &str, max: usize, what: &str) -> Result<usize, String> {
    let parsed: usize = value
        .trim()
        .parse()
        .map_err(|_| format!("`{value}` is not a whole number"))?;

    if parsed == 0 {
        return Err(format!("{what} must be at least 1"));
    }

    if parsed > max {
        return Err(format!("{what} must be at most {max}"));
    }

    Ok(parsed)
}

/// Parses the `rdm queue retry` target.
pub fn parse_retry_target(value: &str) -> Result<RetryTarget, String> {
    let trimmed = value.trim();

    match trimmed.to_ascii_lowercase().as_str() {
        "failed" | "f" => Ok(RetryTarget::Failed),
        "skipped" | "s" => Ok(RetryTarget::Skipped),
        _ => trimmed
            .parse::<u64>()
            .map(RetryTarget::Id)
            .map_err(|_| format!("expected an item ID, `failed` or `skipped`, got `{trimmed}`")),
    }
}

/// Normalises `--ext` values into the set [`crate::sync`] expects.
///
/// Accepts repeated flags and comma separated lists, tolerates a leading dot,
/// and is case insensitive. Returns `None` when no usable extension was given,
/// so callers treat it as "no filter" rather than as an empty set that matches
/// nothing.
pub fn normalize_extensions(raw: &[String]) -> Option<HashSet<String>> {
    let set: HashSet<String> = raw
        .iter()
        .flat_map(|value| value.split(','))
        .map(|ext| ext.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|ext| !ext.is_empty())
        .collect();

    if set.is_empty() { None } else { Some(set) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_trims_and_validates() {
        assert_eq!(
            parse_url("  https://e.com/f.zip ").as_deref(),
            Ok("https://e.com/f.zip")
        );
        assert!(parse_url("ftp://e.com/f.zip").is_err());
        assert!(parse_url("").is_err());
    }

    #[test]
    fn parse_url_hint_has_no_stray_braces() {
        let err = parse_url("example.com/f.zip").expect_err("expected rejection");
        assert!(err.contains("https://example.com/f.zip"), "got: {err}");
        assert!(!err.contains('{') && !err.contains('}'), "got: {err}");
    }

    #[test]
    fn parse_bounded_edges() {
        assert_eq!(parse_connections("1"), Ok(1));
        assert_eq!(
            parse_connections(&MAX_CONNECTIONS.to_string()),
            Ok(MAX_CONNECTIONS)
        );
        assert!(parse_connections(&(MAX_CONNECTIONS + 1).to_string()).is_err());
        assert_eq!(parse_parallel("1"), Ok(1));
        assert_eq!(parse_parallel(&MAX_PARALLEL.to_string()), Ok(MAX_PARALLEL));
        assert!(parse_parallel(&(MAX_PARALLEL + 1).to_string()).is_err());
    }

    #[test]
    fn parse_retry_target_is_case_insensitive() {
        assert_eq!(parse_retry_target("FAILED"), Ok(RetryTarget::Failed));
        assert_eq!(parse_retry_target(" 42 "), Ok(RetryTarget::Id(42)));
    }

    #[test]
    fn extensions_are_normalised() {
        let raw = vec![".FLAC".to_owned(), " mkv ".to_owned()];
        let set = normalize_extensions(&raw).expect("expected a filter");
        assert_eq!(set.len(), 2);
        assert!(set.contains("flac"));
        assert!(set.contains("mkv"));
    }

    #[test]
    fn extensions_split_on_commas_and_dedupe() {
        let raw = vec!["flac,mkv".to_owned(), "FLAC".to_owned()];
        let set = normalize_extensions(&raw).expect("expected a filter");
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn empty_extensions_mean_no_filter() {
        assert_eq!(normalize_extensions(&[]), None);
        assert_eq!(normalize_extensions(&[" ".to_owned()]), None);
        assert_eq!(
            normalize_extensions(&[",".to_owned(), ".".to_owned()]),
            None
        );
    }
}
