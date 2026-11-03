# DistributedFS v1.0.0 (November 2026)

DistributedFS is a small distributed file store in plain Java (JDK 23, no libraries, no build tool), built one tested step at a time as a learning project. Files are split into 1 MiB chunks, each checked with SHA-256; every chunk is stored twice, on different storage node processes; heartbeats detect a failed node, an on-demand repair restores missing and damaged copies, and a browser console shows the cluster live. Uploads and deletes are crash-safe: a write-ahead marker and a single commit point let the server finish or undo any interrupted operation when it starts. It runs on one machine (`127.0.0.1`) and has been tested on Windows only.

## Features

- **Chunking with SHA-256.** Files stream through fixed-size buffers in 1 MiB chunks. Each chunk's SHA-256 is recorded at upload, and a download checks every chunk and the whole file before sending a single byte. Damaged data is reported, never served.
- **Networked storage nodes.** Each node is its own Java process with its own folder, ID and lock, reached over HTTP. A node that cannot be reached is "could not check", never "absent".
- **2× replication.** Two copies of every chunk, on different nodes, so one node can be down without losing data or stopping uploads. A read skips a missing or damaged copy and uses the other.
- **Heartbeats and repair.** Nodes are checked every 2 seconds and marked DOWN after 3 failed checks in a row. `POST /cluster/repair`, with a dry run, restores 2 verified copies of every chunk and trims extras only when every node is UP.
- **Degraded start.** The server starts with a recorded node down if at least 2 of 3 nodes answer. Recovery never treats a chunk as absent while a node that may hold it is down.
- **Crash-safe uploads and deletes.** A pending marker comes first and the manifest is the commit point, written for an upload and removed for a delete. Startup recovery finishes or undoes whatever a crash interrupted, and refuses to start if it can't tell what is safe.
- **Web console.** Plain HTML, CSS and JavaScript served by the server under a strict Content-Security-Policy. It shows the nodes, a chunk map, the files with their health, and an activity feed. You can upload, download, delete (after a confirmation) and run repairs from it.
- **File names.** A file keeps its original name, any script and emoji included, and downloads under it. Names leave the server encoded, so none can inject a header.
- **Cross-site protection.** The server answers only requests addressed to `127.0.0.1` or `localhost`, which blocks DNS rebinding. It refuses every request other than GET whose `Origin` is another site's, and storage nodes refuse any request that carries an `Origin`.

## Benchmarks

Medians of 3 runs of `scripts\bench.ps1` on one laptop (Intel Core i5-13450HX, 16 GB RAM, Windows 11, JDK 23.0.1). The cluster had 3 nodes with 2 copies per chunk, all on `127.0.0.1`.

| Measure | Median |
|---|---|
| Upload of a 64 MiB file | 28.0 MiB/s (2.28 s) |
| Download of that file (every chunk and the whole file checked first) | 67.1 MiB/s (0.95 s) |
| Node killed until `GET /cluster/status` shows it DOWN | 5.20 s |
| `POST /cluster/repair` until every chunk has 2 copies again (44 copies) | 1.69 s |

These are loopback numbers from one machine, measured on 3 November 2026 on the v1.0.0 code; see [BENCHMARKS.md](BENCHMARKS.md).

## Tests

- **48 test suites**, plain-Java runners with no test framework: **781 normal tests** and **10 stress tests**. The stress tests move 64 to 100 MiB through a 16 MiB Java heap. GitHub Actions runs both on Windows.
- **Deliberate breaks:** Every safety rule is backed by deliberate-break checks (RB1–RB109), each confirmed to make a test fail; the per-milestone record is in [TESTING.md](TESTING.md).
- **Browser check:** `scripts\console-smoke.ps1` drives headless Chrome through the console with 56 checks. These cover the headers, layout at desktop and phone widths, text contrast, uploads and downloads under the file's own name, a delete through the page, and a node failure.
- **Crash tests** kill the server in the middle of an upload and at each step of a delete, with local node folders and with node processes.

## Run the demos

With JDK 23 on Windows, from the repository folder:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\demo.ps1          # upload a 5 MiB file, download it, compare SHA-256
powershell -ExecutionPolicy Bypass -File scripts\kill-demo.ps1     # kill a node: downloads and uploads go on
powershell -ExecutionPolicy Bypass -File scripts\repair-demo.ps1   # kill a node, detect it, repair, restart it, trim
powershell -ExecutionPolicy Bypass -File scripts\console-demo.ps1  # start a cluster and open the web console (k, r, q keys)
powershell -ExecutionPolicy Bypass -File scripts\test.ps1          # build, then all 781 normal tests
```

Each demo builds into a new temporary folder and starts three storage nodes and the server on free ports. It stops everything and deletes its folder afterwards, and exits with code 0 only if every check passed.

## Known limitations

- **A learning project, not production software.** There is no authentication, no TLS (plain HTTP on `127.0.0.1` only) and no storage quotas. Everything runs on one machine, and it has been tested on Windows only.
- **No fsync.** Recently written data may be lost if the power fails.
- **Repair is manual.** A lost or damaged copy is made again only when someone asks for a repair; nothing checks copies in the background.
- **A degraded start needs 2 of the 3 nodes.** Some recovery work waits until the missing node is back.
- **A delete with a node down keeps every copy for a while.** The file is gone at once, but its copies are removed only when every node answers. Deletes cannot be undone.
- **The console counts copies by name.** A damaged copy looks good there until a download or a repair finds it.
- **Stalled request headers can pin one of the server's 4 worker threads** until the client disconnects.

Every limitation, in detail: [DESIGN.md](DESIGN.md#known-limitations).
