# MEGA

mega.nz file links work anywhere a normal URL does:

```
rdm 'https://mega.nz/file/AbCdEfGh#your-key-here'
rdm queue add 'https://mega.nz/file/AbCdEfGh#your-key-here'
```

**Quote the link.** The `#` starts a comment in every shell you are likely to be using, and everything after it is the decryption key — an unquoted link silently becomes an unusable one.

What happens under the hood: the key is unpacked from the fragment and never leaves your machine, MEGA hands out a short-lived temporary URL, the file is fetched in parallel chunks and decrypted as it streams to disk, and the result is checked against the MAC embedded in the link before the `.mctemp` file is renamed into place. A failed check is a failed download — you get an error, not a corrupt file.

A few things worth knowing:

- **You do not name the file.** The real filename is encrypted inside the link, so `-o` is optional and only needed to override it. Without `-o` the file lands in `download_dir` under its actual name.
- **`-c` sets the number of chunk workers**, same as it sets connections everywhere else.
- **Interrupted downloads resume.** Progress lives in `<file>.mctemp` plus a small sidecar; rerun the same command and it picks up where it stopped.
- **Queued MEGA links run one at a time**, regardless of `-p`. MEGA's bandwidth quota is per-IP, so downloading three at once is not faster — it just hits the limit three times as often.
- **If you hit the quota** (HTTP 509), rdm backs off and waits rather than failing. Connect a VPN and it notices the new IP and resumes early instead of sitting out the rest of the timer. Turn that off with `mega_resume_on_ip_change = false`.
- **Folder links are not supported yet** — only `/file/` links. A folder link gets a clear error rather than a confusing one.
