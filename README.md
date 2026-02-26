# Rust Download Manager
- A "usable" download manager. Emphasis on usable.
- Resume support on most connections like other download managers.
- Written in rust so it must be cool. Emphasis on must.
- Linux only.

# Example Config File

```
# Parallel connections per file
connections = 12

# Default download directory
download_dir = "~/Downloads"

# Max Retries?
max_retries = 69

# multi-file download at once
queue_parallel = 5
```

# Release
Zack encourages you to build from source. As some random internet person once said, "Always build from source"

# Build from source
```
git clone https://github.com/MasterZack69/rdm
cd rdm
cargo build --release
```

Credits
- MasterZack69 - Of course I am getting the Credits
- Claude 4.6 Opus - Wrote the code, found the bugs
- GPT 5.2 - Asked “what if it races?” one too many times
