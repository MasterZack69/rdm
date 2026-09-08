//! SFTP URLs identify files; they never carry passwords or shell commands.

use anyhow::{Context, Result, ensure};
use reqwest::Url;
use std::path::{Path, PathBuf};

use super::local::STATE_DIR;

#[derive(Clone, Debug)]
pub struct SftpUrl {
    url: Url,
    host: String,
    username: String,
    path: PathBuf,
}

pub fn is_sftp_url(value: &str) -> bool {
    value
        .trim()
        .split_once(':')
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("sftp"))
}

impl SftpUrl {
    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        ensure!(
            !value.chars().any(char::is_control),
            "SFTP URL contains control characters"
        );
        let (_, rest) = value
            .split_once("://")
            .context("Expected sftp://user@host/path")?;
        let (authority, raw_path) = rest.split_once('/').unwrap_or((rest, ""));
        if let Some((userinfo, _)) = authority.rsplit_once('@') {
            ensure!(
                !userinfo.contains(':'),
                "Passwords in SFTP URLs are not supported; use an SSH agent or RDM_SFTP_PASSWORD"
            );
        }
        // Check before Url removes dot segments, including encoded traversal.
        for component in raw_path.split('/').filter(|part| !part.is_empty()) {
            validate_name(&decode(component)?)?;
        }
        let mut url = Url::parse(value).context("Invalid SFTP URL")?;
        ensure!(url.scheme() == "sftp", "Expected an SFTP URL");
        ensure!(url.password().is_none(), "Passwords must not appear in SFTP URLs");
        ensure!(
            url.query().is_none() && url.fragment().is_none(),
            "SFTP URLs cannot contain queries or fragments; encode literal ? and # in filenames"
        );
        let host = url.host_str().context("SFTP URL has no host")?;
        ensure!(
            !host.is_empty()
                && host.is_ascii()
                && !host.contains('%')
                && !host.chars().any(char::is_whitespace),
            "Invalid SFTP hostname"
        );
        let host = host.trim_start_matches('[').trim_end_matches(']').to_owned();
        let username = decode(url.username())?;
        ensure!(
            !username.is_empty() && !username.chars().any(char::is_control),
            "An explicit SFTP username is required"
        );
        let port = url.port().unwrap_or(22);
        ensure!(port != 0, "SFTP port must not be zero");
        url.set_port(Some(port))
            .map_err(|()| anyhow::anyhow!("Invalid SFTP port"))?;
        let mut path = PathBuf::from("/");
        for segment in url.path().split('/').filter(|part| !part.is_empty()) {
            let name = decode(segment)?;
            validate_name(&name)?;
            path.push(name);
        }
        let parsed = Self { url, host, username, path };
        parsed.with_path(parsed.path())
    }

    pub fn as_str(&self) -> &str {
        self.url.as_str()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.url.port().unwrap_or(22)
    }

    pub(crate) fn address_url(&self) -> &Url {
        &self.url
    }

    pub(crate) fn folder_name(&self) -> &str {
        self.path.file_name().and_then(|name| name.to_str()).unwrap_or("sftp")
    }

    pub(crate) fn with_path(&self, path: &Path) -> Result<Self> {
        let text = path.to_str().context("Non-UTF-8 SFTP paths are not supported")?;
        ensure!(text.starts_with('/'), "SFTP paths must be absolute");
        ensure!(text.len() <= 4096, "SFTP path exceeds 4096 bytes");
        let names: Vec<&str> = text.split('/').filter(|name| !name.is_empty()).collect();
        for name in &names {
            validate_name(name)?;
        }
        let mut url = self.url.clone();
        // This setter escapes literal '%' too; set_path would preserve it.
        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("SFTP URL cannot hold a path"))?
            .clear()
            .extend(names);
        Ok(Self {
            url,
            host: self.host.clone(),
            username: self.username.clone(),
            path: path.to_path_buf(),
        })
    }
}

pub(crate) fn validate_name(name: &str) -> Result<()> {
    ensure!(!name.is_empty() && name != "." && name != "..", "Invalid SFTP path component");
    ensure!(name != STATE_DIR, "The .rdm-sftp directory is reserved for transfer state");
    ensure!(
        !name.contains('/') && !name.contains('\\') && !name.chars().any(char::is_control),
        "Unsafe SFTP path component"
    );
    ensure!(name.len() <= 255, "SFTP filename exceeds 255 bytes");
    Ok(())
}

fn decode(value: &str) -> Result<String> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut input = value.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let high = input.next().and_then(|b| char::from(b).to_digit(16));
            let low = input.next().and_then(|b| char::from(b).to_digit(16));
            let (Some(high), Some(low)) = (high, low) else {
                anyhow::bail!("Invalid percent escape in SFTP URL");
            };
            bytes.push((high * 16 + low) as u8);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).context("SFTP URL must decode to UTF-8")
}
