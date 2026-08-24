//! The requests this module makes, and how it explains a refusal.
//!
//! Every one of them is a plain GET returning JSON, which is why there is no
//! request-builder closure here of the kind OneDrive needs: there are no
//! bodies and no per-attempt headers, so there is nothing to rebuild between
//! attempts.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;

/// pixeldrain's error body.
///
/// Worth parsing rather than printing raw: `value` is a stable code and
/// `message` is a sentence written for a person, both of which beat "HTTP
/// 404" by a distance.
#[derive(Debug, Deserialize)]
struct ApiError {
    #[serde(default)]
    value: String,
    #[serde(default)]
    message: String,
}

/// Builds the client every request in this module goes out over, including the
/// file transfer itself.
///
/// The API key, when there is one, lives in this client's default headers
/// rather than in the URL. The engine downloads over the client it is handed,
/// and a credential in a URL would end up in `.rdm` resume state, in shell
/// history and on the progress line.
///
/// pixeldrain's documented scheme is HTTP Basic with the key as the *password*
/// and the username ignored, so the header is built here rather than with
/// `RequestBuilder::basic_auth`, which is per-request and would not survive
/// the hand-off to the engine.
///
/// Note the absence of an `Accept` header: this same client fetches file
/// bytes, and `application/json` would be a lie on that request.
pub(super) fn build_client(api_key: Option<&str>) -> Result<Client> {
    let mut headers = HeaderMap::new();

    if let Some(key) = api_key {
        let encoded = STANDARD.encode(format!(":{key}"));
        let mut value = HeaderValue::from_str(&format!("Basic {encoded}"))
            .context("the pixeldrain API key cannot be put in an HTTP header")?;
        // Keeps the key out of any `Debug` of the client or its headers, which
        // is the shape a bug report tends to arrive in.
        value.set_sensitive(true);
        headers.insert(AUTHORIZATION, value);
    }

    Client::builder()
        .user_agent("rdm")
        .connect_timeout(Duration::from_secs(10))
        .default_headers(headers)
        .build()
        .context("could not build the pixeldrain HTTP client")
}

/// GETs `endpoint` until it answers with JSON that parses, `retries` attempts
/// at most.
///
/// `what` is what the caller was trying to do, and becomes the outermost line
/// of the error, so the reason underneath it reads as an explanation rather
/// than as an isolated complaint.
pub(super) async fn fetch_json<T>(
    client: &Client,
    retries: u32,
    what: &'static str,
    endpoint: &str,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut last: Option<anyhow::Error> = None;

    for attempt in 0..retries.max(1) {
        match client.get(endpoint).send().await {
            Ok(response) => {
                let status = response.status();

                match response.bytes().await {
                    Ok(body) if status.is_success() => match serde_json::from_slice(&body) {
                        Ok(parsed) => return Ok(parsed),
                        Err(error) => {
                            // Almost always a captive portal or a block page
                            // answering 200 with HTML, so the body is the
                            // useful half of the message.
                            last = Some(anyhow!(error).context(format!(
                                "pixeldrain answered with something that is not the JSON asked for: {}",
                                snippet(&body)
                            )));
                        }
                    },
                    Ok(body) => {
                        last = Some(anyhow!("{}", explain(status, &body)));
                        // A refusal is a decision, not a hiccup: asking five
                        // more times gets the same answer more slowly.
                        if is_final(status) {
                            break;
                        }
                    }
                    Err(error) => {
                        last = Some(anyhow!(error).context("the response could not be read"));
                    }
                }
            }
            Err(error) => last = Some(anyhow!(error)),
        }

        backoff(attempt).await;
    }

    Err(last
        .unwrap_or_else(|| anyhow!("pixeldrain did not answer"))
        .context(what))
}

/// Turns a refusal into something worth reading.
///
/// pixeldrain answers failures with a JSON body carrying a stable code and a
/// sentence, so the body is the message and the status code is only what is
/// left when there is no body to read.
fn explain(status: StatusCode, body: &[u8]) -> String {
    if let Ok(error) = serde_json::from_slice::<ApiError>(body) {
        let message = error.message.trim();
        let code = error.value.trim();

        if !message.is_empty() {
            // The code is kept alongside the sentence because it is the half
            // that is stable enough to search the API docs for.
            return match code.is_empty() {
                true => format!("pixeldrain: {message}"),
                false => format!("pixeldrain: {message} ({code})"),
            };
        }

        if !code.is_empty() {
            return format!("pixeldrain answered '{code}' (HTTP {status})");
        }
    }

    format!("pixeldrain answered HTTP {status}: {}", snippet(body))
}

/// Is this status worth another attempt?
///
/// 401 is in the list because a rejected API key will go on being rejected,
/// and retrying it four more times only delays the message that says so.
fn is_final(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::NOT_FOUND
            | StatusCode::GONE
            | StatusCode::UNPROCESSABLE_ENTITY
    )
}

/// Enough of a response body to recognise it, and no more.
///
/// Bodies end up in error messages and error messages end up in terminals, so
/// an HTML error page is worth one line rather than four hundred. Whitespace
/// is collapsed for the same reason.
fn snippet(body: &[u8]) -> String {
    const LIMIT: usize = 160;

    let text = String::from_utf8_lossy(body);
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");

    if collapsed.is_empty() {
        return "(empty response)".to_owned();
    }

    match collapsed.char_indices().nth(LIMIT) {
        Some((cut, _)) => format!("{}\u{2026}", &collapsed[..cut]),
        None => collapsed,
    }
}

/// Waits before another attempt: 400 ms, 800 ms, 1.6 s, and so on to a 5 s
/// ceiling.
async fn backoff(attempt: u32) {
    let millis = 400u64.saturating_mul(1u64 << attempt.min(4));
    tokio::time::sleep(Duration::from_millis(millis.min(5_000))).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_is_reported_in_pixeldrains_own_words() {
        let body =
            br#"{"success":false,"value":"file_not_found","message":"The file does not exist."}"#;
        assert_eq!(
            explain(StatusCode::NOT_FOUND, body),
            "pixeldrain: The file does not exist. (file_not_found)"
        );
    }

    #[test]
    fn a_refusal_with_only_a_code_still_names_it() {
        let body = br#"{"success":false,"value":"out_of_bandwidth"}"#;
        assert_eq!(
            explain(StatusCode::FORBIDDEN, body),
            "pixeldrain answered 'out_of_bandwidth' (HTTP 403 Forbidden)"
        );
    }

    #[test]
    fn a_block_page_does_not_get_to_fill_the_terminal() {
        let body = format!("<html>{}</html>", "x ".repeat(500));
        let message = explain(StatusCode::BAD_GATEWAY, body.as_bytes());
        assert!(message.ends_with('\u{2026}'), "{message}");
        assert!(message.chars().count() < 240, "{message}");
    }

    #[test]
    fn an_empty_body_says_so_rather_than_saying_nothing() {
        assert_eq!(
            explain(StatusCode::BAD_GATEWAY, b""),
            "pixeldrain answered HTTP 502 Bad Gateway: (empty response)"
        );
    }

    #[test]
    fn a_rejected_key_is_not_retried() {
        // Retrying a 401 only delays the message that explains it.
        assert!(is_final(StatusCode::UNAUTHORIZED));
        assert!(is_final(StatusCode::NOT_FOUND));
        assert!(!is_final(StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_final(StatusCode::BAD_GATEWAY));
    }

    #[test]
    fn a_client_without_a_key_carries_no_authorization_header() {
        // The anonymous path must not send an empty `Basic OG==` and invite a
        // 401 where none was needed.
        assert!(build_client(None).is_ok());
        assert!(build_client(Some("deadbeef")).is_ok());
        // Keys arrive from a config file, so a newline in one is a typo rather
        // than an attack, and it should be a message and not a panic.
        assert!(build_client(Some("bad\nkey")).is_err());
    }
}
