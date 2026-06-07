# Rust Download Manager
- A "usable" download manager. Emphasis on usable.
- Resume support on most connections like other download managers.
- Efficient as Fuck.
- Written in rust so it must be cool. Emphasis on must.
- Linux only.

# HoW To uSe 
```
RDM — Rust Download Manager

Usage:
  rdm <URL>                                Quick download
  rdm download <URL> [-o name] [-c N]      Download with options
  rdm sync <URL> [-d] [-e flac,mkv]        Sync remote → local
  rdm queue <command>                      Manage download queue
  rdm config                               Show configuration

Queue commands:
  rdm queue add <URL> [-o name] [-c N]     Add to queue
  rdm queue list                           Show queue
  rdm queue start [-p N]                   Start processing
  rdm queue stop / skip                    Live control

Options:
  -c, --connections N    Connections per file (default: 8)
  -o, --output NAME      Output file or directory
  -p, --parallel N       Parallel queue downloads (default: 5)
  -e, --ext EXT,...      Sync: only sync these file extensions
  -d, --delete           Sync: remove local files not on remote
  --allow-private        Allow scanning private/local IP addresses
```

If you have set up the path variable like a normal person then you can reproduce the above wall of text by typing 'rdm', assuming the variable is rdm.

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

# Credits
- MasterZack69 - Of course I am getting the Credits
- Claude Opus 4.6 - Wrote the code, fixed the bugs
- GPT 5.2 - Asked “what if it races?” one too many times
- Claude Opus 4.7 - Here to do everything better
