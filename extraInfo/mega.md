# MEGA

mega.nz file links work anywhere a normal URL does:

```
rdm 'https://mega.nz/file/AbCdEfGh#your-key-here'
rdm queue add 'https://mega.nz/file/AbCdEfGh#your-key-here'
rdm sync 'https://mega.nz/file/AbCdEfGh#your-key-here'
```

**Quote the link.** `#` starts a shell comment, and everything after it is the decryption key — an unquoted link silently becomes an unusable one.

The key is unpacked from the fragment and never leaves your machine. MEGA hands out a short-lived temporary URL, the file is fetched in parallel chunks and decrypted as it streams to disk, and the result is checked against the MAC embedded in the link before the `.mctemp` file is renamed into place. A failed check is a failed download, not a corrupt file.

Notes:

- **You do not name the file.** The real filename is encrypted inside the link, so `-o` is only needed to override it.
- **Queued MEGA links run one at a time**, regardless of `-p`. The quota is per-IP, so three at once is not faster — it just hits the limit three times as often.
- **On quota (HTTP 509)** rdm backs off and waits. Connect a VPN and it notices the new IP and resumes early.

