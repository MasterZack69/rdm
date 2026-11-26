# Download performance comparisons

`download_gate.py` runs tools sequentially against the same files. It rotates
execution order in each round, checks the output size and SHA-256 outside the
timed interval, and writes results after every trial. Python 3 is the only
harness dependency. **A fast invalid download is a failure, not a result.**

Use at least two independently hosted, stable, range-capable files and a file
representative of your actual workload. Run enough rounds to balance positions
and repeat promising results in a separate session. A single CDN or a handful
of noisy samples is not a release decision. Respect server policies and
bandwidth limits. Use fresh output files and the same machine, filesystem,
connection count, timeouts, tool versions, and otherwise idle environment.

## Private configuration

Make one local text file per target containing only its URL. Never commit a
signed URL or put it in a label. Calculate each expected full-file SHA-256 and
size independently before the comparison. Use stable files: if the server
changes content the run will stop on verification failure. Create a private
JSON config outside the repository:

```json
{
  "targets": [
    {
      "label": "provider-a",
      "url_file": "/private/path/provider-a-url.txt",
      "size_bytes": 104857600,
      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
    }
  ],
  "tools": [
    {"label": "main", "argv": ["/path/to/main/rdm", "-q", "-c", "8", "-o", "{output}", "{url}"]},
    {"label": "candidate", "argv": ["/path/to/candidate/rdm", "-q", "-c", "8", "-o", "{output}", "{url}"]}
  ]
}
```

Replace the example digest and size. Add other targets and competitors as
needed. Each command is an argument array, never a shell command; exactly one
`{output}` and `{url}` placeholder are required. Tools must write the full
result to `{output}`. Labels are short, non-sensitive identifiers. Do not put
secrets in labels. The config is trusted local input and may execute the
specified programs; protect it accordingly.

Each trial uses a clean temporary HOME/XDG directory. By default only `PATH`
and `LANG` are inherited; proxies and credentials are not. If a particular
workload requires environment settings, add an `inherit_env` array of variable
**names** to that tool, and set their values in the launching environment.
Values and command arrays are not saved to the results. A SHA-256 fingerprint
of each tool's executable is recorded. If a tool is launched via a wrapper or
loader, set `fingerprint_file` to the actual executable path. URLs are nevertheless
passed to downloaders as process arguments and may be visible to local users
who can inspect processes. Configure file permissions and machine access
accordingly.

```sh
python3 benchmarks/download_gate.py \
  --config /private/path/gate.json --results /private/path/results.json \
  --baseline main --rounds 8 --workdir /path/on/test/filesystem
```

Results contain per-trial timings, labels, sizes, hashes, exit status, and
paired summaries, but not URLs or captured subprocess output. A failed,
timed-out or corrupt trial is retained and stops the run; investigate it
separately rather than silently discarding it. The results path must be new.
`--seed` makes the counterbalanced ordering reproducible. The temporary
files are deleted as each run ends. The output reports comparisons; it does
not automatically approve code or prove statistical significance. Check
resume, cancellation, errors, integrity and the full test suite separately.

Run the harness tests with:

```sh
python3 -m unittest discover -s benchmarks -p 'test_*.py' -v
```

## Controlled overload regression

`overload_gate.py` runs a local HTTP/1.1 server. The first eight workers each
receive one 429; afterward the server accepts at most four concurrent requests
and rejects extras. It is a deterministic stress case for retry admission,
not a model of every real server. It records successful file hashes, elapsed
time and the number of 429s for both binaries.

```sh
python3 benchmarks/overload_gate.py --main /path/to/main/rdm \
  --candidate /path/to/candidate/rdm --results /private/path/overload.json \
  --workdir /path/on/test/filesystem --rounds 4
```
