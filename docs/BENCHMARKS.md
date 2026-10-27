# DistributedFS: benchmarks

Written by `scripts\bench.ps1` on 2026-10-27 17:54 +05:30. These numbers describe one machine on one day; they are not run in CI and not a guarantee for other environments. See the [README](../README.md) for the overview.

## Setup

- **Cluster:** 3 storage node processes and the server, all on `127.0.0.1`, replication factor 2, default 1 MiB chunks. Each run starts a fresh cluster in a new temporary folder.
- **File:** 64 MiB of random bytes, uploaded with `POST /files` and downloaded with `GET /files/<id>`; the download's SHA-256 is checked against the upload. A 1 MiB upload and download before each run warm the JVMs up and are not measured.
- **Upload time:** from sending the request until the `201` answer, which comes after both copies of every chunk and the manifest are stored. The file is read into memory first, so reading it is not counted.
- **Download time:** from the request until the whole file is on disk. The server checks every chunk and the whole file before it sends the first byte, so this includes that check.
- **Kill to DOWN:** node 1 is killed; the clock starts when its process has exited, and `GET /cluster/status` is polled every 100 ms until it shows node 1 DOWN. The heartbeat checks every 2 seconds and needs 3 failed checks in a row, so this is expected to be roughly 4 to 6 seconds, depending on where in the 2-second cycle the kill falls.
- **Repair to 2 copies:** the time of `POST /cluster/repair` while node 1 is DOWN; the request answers when the repair has finished. Afterwards every chunk is checked to have a copy on node 0 and on node 2.
- **Runs:** 3; the tables give each run and the median.

## System

| | |
|---|---|
| CPU | 13th Gen Intel(R) Core(TM) i5-13450HX (10 cores, 16 logical processors) |
| RAM | 15.7 GiB |
| OS | Microsoft Windows 11 Home Single Language 10.0.26200 (build 26200) |
| JDK | java version "23.0.1" 2024-10-15; Java(TM) SE Runtime Environment (build 23.0.1+11-39); Java HotSpot(TM) 64-Bit Server VM (build 23.0.1+11-39, mixed mode, sharing) |
| PowerShell | 5.1.26100.9711 |
| Source | `8535e73-dirty` |

## Medians of 3 runs

| Measure | Median |
|---|---|
| Upload throughput | 26.2 MiB/s (2.44 s) |
| Download throughput | 63.3 MiB/s (1.01 s) |
| Kill to DOWN | 5.31 s |
| Repair to 2 copies | 1.69 s (44 copies added) |

## Every run

| Run | Upload | Download | Kill to DOWN | Repair | Copies added | Repair, server's `durationMillis` |
|---|---|---|---|---|---|---|
| 1 | 2.44 s, 26.2 MiB/s | 1.06 s, 60.4 MiB/s | 5.29 s | 1.64 s | 44 | 1613 ms |
| 2 | 2.38 s, 26.9 MiB/s | 0.95 s, 67.2 MiB/s | 5.62 s | 1.69 s | 44 | 1673 ms |
| 3 | 2.63 s, 24.3 MiB/s | 1.01 s, 63.3 MiB/s | 5.31 s | 1.88 s | 44 | 1871 ms |

## Caveats

- Everything runs on one machine over loopback, so these are not network numbers: there is no real network latency or bandwidth limit, and the nodes share the CPU and the disk.
- The operating system's file cache holds recently written chunks, so reads may not touch the disk.
- The client is PowerShell's `WebClient`, and its own overhead is included.
- Kill-to-DOWN is limited by the 100 ms polling and by the heartbeat's fixed 2-second interval.
