# DistributedFS: testing

How the tests are built and run, what continuous integration checks, and the results recorded for each milestone. See the [README](../README.md) for the overview and [DESIGN.md](DESIGN.md) for the design.

## Run the tests

Each suite is a plain-Java program. It prints `PASS`, `FAIL` or `SKIP` for every test and ends with a summary such as `18 passed, 0 failed`. A suite exits with code 1 if any test fails.

| Suite | Normal tests | Stress tests (run separately) |
|---|---|---|
| `ApiErrorTest` | 5 | none |
| `ChecksumServiceTest` | 15 | 1 |
| `ClusterStatusEndpointTest` | 11 | none |
| `CrashRecoveryTest` | 8 | 1 |
| `DataDirectoryTest` | 20 | none |
| `DegradedStartupTest` | 15 | none |
| `DistributedFsDemoTest` | 18 | 1 |
| `DistributedFsServerTest` | 28 | none |
| `DownloadEndpointTest` | 26 | 1 |
| `FileChunkerTest` | 18 | none |
| `FileIdsTest` | 12 | none |
| `FileReconstructorTest` | 15 | 1 |
| `FileServiceTest` | 34 | none |
| `FileTreesTest` | 5 | none |
| `HeartbeatMonitorTest` | 14 | none |
| `JsonTest` | 8 | none |
| `ManifestStoreTest` | 28 | none |
| `NetworkedRecoveryTest` | 8 | none |
| `NodeApiTest` | 26 | none |
| `NodeDataDirectoryTest` | 11 | none |
| `RemoteClusterTest` | 10 | none |
| `RemoteServerTest` | 11 | none |
| `RemoteStorageNodeTest` | 20 | 1 |
| `RepairEndpointTest` | 9 | none |
| `RepairServiceTest` | 19 | none |
| `ReplicatedClusterTest` | 24 | none |
| `ReplicatedRecoveryTest` | 12 | none |
| `ReplicatedServerTest` | 13 | none |
| `StaleTempFilesTest` | 4 | none |
| `StalledRequestTest` | 16 | none |
| `StallWatchTest` | 6 | none |
| `StartupRecoveryTest` | 27 | none |
| `StorageClusterTest` | 45 | 1 |
| `StorageNodeServerTest` | 6 | none |
| `StorageNodeTest` | 58 | 1 |
| `UploadEndpointTest` | 24 | 2 |
| **Total** | **629** | **10** |

The simplest way to build and run everything is the script that continuous integration uses. From the repository folder:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\test.ps1                 # build, then every normal suite
powershell -ExecutionPolicy Bypass -File scripts\test.ps1 -Stress         # build, then the stress tests with -Xmx16m
powershell -ExecutionPolicy Bypass -File scripts\test.ps1 -Suite StorageNodeTest,FileServiceTest
```

Each run compiles into a new temporary folder (never `out` or `out-test`), so classes left over from deleted or renamed sources cannot hide an error, and deletes that folder when it finishes. It prints every suite's output, then a total and the name of every skipped test, and exits with code 1 if compilation or any suite failed. It works in Windows PowerShell 5.1 and PowerShell 7. (`-ExecutionPolicy Bypass` applies to that one command only; it is needed on machines whose policy blocks local scripts.)

To run the suites by hand instead, from the repository folder:

```powershell
foreach ($suite in 'ApiErrorTest', 'ChecksumServiceTest', 'ClusterStatusEndpointTest', 'CrashRecoveryTest',
        'DataDirectoryTest', 'DegradedStartupTest', 'DistributedFsDemoTest', 'DistributedFsServerTest', 'DownloadEndpointTest', 'FileChunkerTest',
        'FileIdsTest', 'FileReconstructorTest', 'FileServiceTest', 'FileTreesTest', 'HeartbeatMonitorTest', 'JsonTest',
        'ManifestStoreTest', 'NetworkedRecoveryTest', 'NodeApiTest', 'NodeDataDirectoryTest', 'RemoteClusterTest',
        'RemoteServerTest', 'RemoteStorageNodeTest', 'RepairEndpointTest', 'RepairServiceTest',
        'ReplicatedClusterTest', 'ReplicatedRecoveryTest',
        'ReplicatedServerTest',
        'StaleTempFilesTest', 'StalledRequestTest', 'StallWatchTest', 'StartupRecoveryTest',
        'StorageClusterTest', 'StorageNodeServerTest', 'StorageNodeTest', 'UploadEndpointTest') {
    java -cp "out;out-test" "com.distributedfs.$suite"
}
```

Or run one suite, for example:

```powershell
java -cp "out;out-test" com.distributedfs.UploadEndpointTest
```

Some tests are reported as `SKIP` instead of passing when the machine can't run them:

- Five tests, two in `StorageNodeTest`, two in `StorageClusterTest` and one in `NodeApiTest`, need to create a symbolic link. Windows allows that only with Developer Mode turned on or with administrator rights. On the Windows 11 machine where the code was verified, the first four were skipped while Developer Mode was off, and all five ran and passed with it turned on (see [Recorded verification results](#recorded-verification-results)). Related checks are also tested with Windows junctions, which don't need special rights, and those tests ran; they don't replace the symbolic-link tests (see [Known limitations](DESIGN.md#known-limitations)).
- One `DistributedFsServerTest` test needs an IPv4 address other than loopback, to show that the server can't be reached through it.

`CrashRecoveryTest`, `DataDirectoryTest`, `NetworkedRecoveryTest` and `StorageNodeServerTest` start extra Java processes with the same class path, the node tests start node servers on free ports of `127.0.0.1`, and several tests create Windows junctions with `cmd.exe /c mklink /J` inside their own temporary folders.

Run the stress tests separately. The `-Xmx16m` option limits the Java heap to 16 MiB, so each test shows that the data streams through without being loaded into memory. Without that option the tests still run, and most print a warning if the heap is 64 MiB or larger:

```powershell
java -Xmx16m -cp "out;out-test" com.distributedfs.FileReconstructorTest --stress
java -Xmx16m -cp "out;out-test" com.distributedfs.ChecksumServiceTest --stress
java -Xmx16m -cp "out;out-test" com.distributedfs.StorageNodeTest --stress
java -Xmx16m -cp "out;out-test" com.distributedfs.StorageClusterTest --stress
java -Xmx16m -cp "out;out-test" com.distributedfs.DistributedFsDemoTest --stress
java -Xmx16m -cp "out;out-test" com.distributedfs.UploadEndpointTest --stress
java -Xmx16m -cp "out;out-test" com.distributedfs.CrashRecoveryTest --stress
java -Xmx16m -cp "out;out-test" com.distributedfs.DownloadEndpointTest --stress
java -Xmx16m -cp "out;out-test" com.distributedfs.RemoteStorageNodeTest --stress
```

`UploadEndpointTest --stress` uploads exactly 100 MiB and then four 20 MiB files at once. `RemoteStorageNodeTest --stress` stores one 64 MiB chunk on a storage node over HTTP and retrieves it again, with the node and its client in the same 16 MiB heap. `CrashRecoveryTest --stress` kills a child process partway through a 64 MiB upload and passes `-Xmx16m` to that child itself. `DownloadEndpointTest --stress` uploads 100 MiB and downloads it again, checking the SHA-256 of the bytes as they arrive.

In PowerShell, keep the quotes around `"out;out-test"`: an unquoted `;` ends the command.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request to `main` (and by hand from the Actions tab). On a `windows-latest` runner with Temurin JDK 23 it reports whether the runner may create symbolic links, then runs `scripts\test.ps1` and `scripts\test.ps1 -Stress`. The job's summary page lists each suite's result and every skipped test by name. It needs no secrets. The tests use Windows junctions, so CI runs on Windows only. Hosted Windows runners usually allow symbolic links, so the four symbolic-link tests may run there even where they are skipped locally; the job log shows whether they did. The workflow was confirmed in a GitHub Actions run on 24 October 2026: it passed with 418 normal tests, 0 skipped, so the symbolic-link tests ran there.

## Recorded verification results

The results below were recorded when each run was made; they are not rerun when these documents change. They describe those runs on that machine, not a guarantee for other environments.

The first recorded run tested the code in commit `0feb7a1` ("Add verified HTTP file downloads"), on Windows 11 with JDK 23.0.1, on 20 October 2026:

- **Normal tests:** all 20 suites exited with code 0. Of the 377 normal tests, 375 passed, 0 failed and 2 were skipped. The two skipped tests are the symbolic-link tests described above; Developer Mode was off and the shell was not elevated.
- **Stress tests,** run separately and not included in the figures above: all 9 passed with `-Xmx16m`, including the 100 MiB download.
- **Deliberate-break checks:** 11 checks each broke one rule of the download code in a separate copy of the source (for example: skip the SHA-256 check, decode the file ID before checking it, read without the storage lock, accept `HEAD`, disable the stall watchdog, keep the download workspace). The tests detected all 11.
- **Not verified:** stopping the server with Ctrl+C. The shutdown hook is covered only indirectly, by tests of the server's `stop()` method, and has not been checked by hand.

A later recorded run tested the request-stall hardening (stalled request bodies and uploads, and the no-follow check before retrieving chunks), the Java source and tests of commit `64ac867`, on the same machine with JDK 23.0.1, on 22 October 2026:

- **Normal tests:** all 21 suites exited with code 0. Of the 398 normal tests, 394 passed, 0 failed and 4 were skipped: the four symbolic-link tests, with Developer Mode off and the shell not elevated.
- **Stress tests,** run separately: all 9 passed with `-Xmx16m`.
- **Regression check:** the new tests were also run against the code from before the change. There, 13 of the 16 `StalledRequestTest` tests and all 3 new junction tests failed, as intended. The 3 `StalledRequestTest` tests that passed cover behavior that was already correct: complete bodies, a slow but steady upload, and the download route.
- **Deliberate-break checks:** 6 checks each broke one new rule in a separate copy of the source (skip finishing the body before a response, ignore chunked bodies, treat every request as having no body, count no upload progress, let the upload watchdog never fire, follow links when retrieving). The tests detected all 6.

A third recorded run tested the per-chunk checksums, the CI script and the link-safety additions (store refuses a junctioned node folder; new dangling-junction delete tests), on the same machine with JDK 23.0.1, on 23 October 2026, before they were committed:

- **Normal tests:** all 21 suites exited with code 0. Of the 417 normal tests, 413 passed, 0 failed and 4 were skipped: the four symbolic-link tests, with Developer Mode off and the shell not elevated. `scripts\test.ps1` was run in Windows PowerShell 5.1 on a copy of the source and gave the same counts; it exited with code 1 on a copy with a deliberately broken rule. GitHub Actions had not yet run the workflow.
- **Stress tests,** run separately: all 9 passed with `-Xmx16m`.
- **Deliberate-break checks:** an earlier check, removing the no-follow option from the storage node's regular-file check (the check deleting relies on), had gone undetected because only the skipped symbolic-link tests could catch it. Against this code it fails 3 tests, two of them new delete tests built on dangling junctions, which need no special rights. Three new checks broke the per-chunk checksums (skip the chunk check, record wrong chunk checksums, remove the chunk-count limit); the tests detected all 3. The new test for a junctioned node folder failed against the code before the fix.

A follow-up run on the same machine on 23 October 2026, also before commit, with Windows Developer Mode turned on, tested the same code plus a fix to the order of `storeChunk`'s checks and a `scripts\test.ps1` that builds into a new temporary folder on each run:

- **Normal tests:** `scripts\test.ps1` in Windows PowerShell 5.1: all 21 suites exited with code 0. Of the 418 normal tests, 418 passed, 0 failed and 0 were skipped. The four symbolic-link tests ran and passed.
- **Stress tests,** run separately with `scripts\test.ps1 -Stress`: all 9 passed with `-Xmx16m`.
- **Deliberate-break checks:** removing the no-follow option from the storage node's regular-file check now fails 7 tests, including all four symbolic-link tests. Putting `storeChunk`'s duplicate check back before its node-folder check fails the new test for that case, which then sees a misleading "already exists" error.
- **Ctrl+C shutdown,** checked manually on Windows 11 with JDK 23: the server printed `DistributedFS server stopped.` after about 2 seconds, restarted immediately on the same data directory without an "already in use" error, and a file uploaded before the restart downloaded byte-for-byte identical.

A run on 24 October 2026, on the same machine with Developer Mode on, tested milestone M1a (the `ChunkStore` interface, the storage node process and its client) before it was committed as `6b98712` and merged in PR #5 (`d399531`):

- **Normal tests:** `scripts\test.ps1` in Windows PowerShell 5.1: all 26 suites exited with code 0. Of the 491 normal tests, 491 passed, 0 failed and 0 were skipped, including all five symbolic-link tests. The run took about 104 seconds.
- **Stress tests,** run separately with `scripts\test.ps1 -Stress`: all 10 passed with `-Xmx16m`, in about 16 seconds.
- **Deliberate-break checks:** 17 checks each broke one rule of the new code in a separate copy of the source. 16 were detected:
  - client side: a timeout or refused connection reported as "absent"; any 404 reported as a missing chunk; the node ID ignored; local chunk-ID checks skipped; a failure after connecting treated as "certainly not stored"; a partial retrieved copy kept; a chunk held in memory (`OutOfMemoryError` under `-Xmx16m`);
  - cluster: another node tried after a store with an unknown outcome;
  - node: chunk-ID rules skipped; the SHA-256 check skipped; a short body published; the `Host` check removed; `mode=nofollow` following links; the node-wide lock removed; the folder lock removed.
  
  The short-body check was at first not detected over HTTP, because the JDK's own request stream and the SHA-256 check both refuse such a body; a direct test of the node's store was added, and it then failed as intended. One check was not detected: removing the client's own check that a chunk's body is as long as its `Content-Length`, because the JDK's HTTP client already refuses such a body before that check runs.

A run on 25 October 2026, on the same machine with Developer Mode on, tested milestone M1b (the server with storage node processes) before it was committed as `7adce90` and merged in PR #6 (`b6afdef`):

- **Normal tests:** `scripts\test.ps1` in Windows PowerShell 5.1: all 28 suites exited with code 0. Of the 510 normal tests, 510 passed, 0 failed and 0 were skipped. The run took about 107 seconds.
- **Stress tests,** run separately with `scripts\test.ps1 -Stress`: all 10 passed with `-Xmx16m`, in about 15 seconds.
- **Deliberate-break checks:** 5 checks each broke one new rule in a separate copy of the source, and the tests detected all 5: a chunk whose store answer was lost left out of the rollback; a node that cannot be listed at startup ignored; the membership file not checked; the same node given twice not refused (a later step still refuses the start, but only with an internal error); a node failure during a download reported as a missing chunk. The check of an unlistable node was at first masked by the per-chunk checks of an interrupted upload, so a test without interrupted uploads was added before the run.
- **`scripts\demo.ps1`:** started three node processes and the server on free ports, uploaded a random 5 MiB file (5 chunks, stored 2, 2 and 1 per node), downloaded it with the same SHA-256 as the original and as recorded by the server, printed `RESULT: SUCCESS`, exited with code 0, and left no process or folder behind.

A later run on 25 October 2026, on the same machine, tested milestone M2 (replication factor 2) before it was committed as `528a756` and merged in PR #7 (`3337474`):

- **Normal tests:** `scripts\test.ps1` in Windows PowerShell 5.1: all 31 suites exited with code 0. Of the 559 normal tests, 559 passed, 0 failed and 0 were skipped. The run took about 114 seconds.
- **Stress tests,** run separately with `scripts\test.ps1 -Stress`: all 10 passed with `-Xmx16m`, in about 16 seconds.
- **Deliberate-break checks:** 16 checks each broke one replication rule in a separate copy of the source, and the tests detected all 16: two copies counted on one node; one copy accepted as enough; another node tried after a lost store answer; the duplicate check skipping every failing node, or no node; a node that times out after connecting treated as unreachable; only the first copy deleted; the marker deleted although a down node could not confirm the rollback; copies read without checking their SHA-256; a read giving up at the first node that is down; a failed node not tried last; damaged copies reported as a missing chunk; 3 copies of a committed chunk accepted; recovery deleting only one copy; under-replicated chunks not reported; the server keeping one copy.
- **`scripts\demo.ps1`** and **`scripts\kill-demo.ps1`:** both printed `RESULT: SUCCESS` and exited with code 0. With node 1 killed, the kill demo downloaded file A intact, stored every chunk of a new file B on nodes 0 and 2, downloaded B intact, and showed the server's warnings about node 1. Neither left a process or folder behind.

A run on 26 October 2026, on the same machine, tested milestone M3 (failure detection and repair) before it was committed:

- **Normal tests:** `scripts\test.ps1` in Windows PowerShell 5.1: all 36 suites exited with code 0. Of the 629 normal tests, 629 passed, 0 failed and 0 were skipped. The run took about 123 seconds.
- **Stress tests,** run separately with `scripts\test.ps1 -Stress`: all 10 passed with `-Xmx16m`, in about 20 seconds.
- **Deliberate-break checks:** 18 checks each broke one M3 rule in a separate copy of the source, and the tests detected all 18: DOWN after 1 failure; an answer with the wrong node ID counted as good; UP needing more than 1 good check; the heartbeat thread left running after `stop()`; the status endpoint accepting `POST`; copying from an unverified copy; a new copy not read back; a holder or DOWN node chosen as the target; trimming with a node DOWN; trimming below 2 copies; deleting the damaged copies of a lost chunk; a dry run that stores copies; two repairs at once; a repair without the storage lock; a degraded start with 1 node; a committed upload's marker deleted although its chunk is only on the missing node; an incomplete upload cleaned up with a node missing; `deleteChunk` deleting only 2 of 3 copies. Two needed better tests first: choosing a DOWN node or a node that already holds a good copy as the target was not detected, because the store fails harmlessly and the next node is used, so two tests now count store attempts on those nodes (each half of the break is detected on its own); and two repairs at once made a test wait forever for a lock instead of failing, so that test now has a time limit.
- **`scripts\demo.ps1`**, **`scripts\kill-demo.ps1`** and **`scripts\repair-demo.ps1`:** all three printed `RESULT: SUCCESS` and exited with code 0. In the repair demo, node 1 was DOWN 5.6 seconds after it was killed; the dry run changed nothing; the repair added 4 copies; node 1 was UP 1.5 seconds after its restart; the next repair removed the 4 extra copies; and the file downloaded with the original SHA-256. None left a process or folder behind.

A run on 27 October 2026, on the same machine, tested milestone M4a (this documentation split, `scripts\bench.ps1`, and `DownloadEndpointTest`'s socket time limit raised from 10 to 30 seconds) before it was committed:

- **Normal tests:** `scripts\test.ps1` in Windows PowerShell 5.1: all 36 suites exited with code 0. Of the 629 normal tests, 629 passed, 0 failed and 0 were skipped. The run took about 129 seconds. No Java code outside that test constant changed, so the stress tests were not run again.
- **`scripts\demo.ps1`**, **`scripts\kill-demo.ps1`** and **`scripts\repair-demo.ps1`:** all three printed `RESULT: SUCCESS` and exited with code 0; in the repair demo, node 1 was DOWN 5.1 seconds after it was killed and UP 1.5 seconds after its restart. Excerpts are in the [README](../README.md#what-the-demos-print).
- **`scripts\bench.ps1`:** 3 runs, all checks passed; the results are in [BENCHMARKS.md](BENCHMARKS.md).

## Running tests and contributing

Personal learning project; issues and suggestions welcome.

- Each `*Test` class is a small test runner with its own `main` method; no test framework is used. Every test that touches files runs in its own temporary folder, which is deleted afterwards, and a suite exits with code 1 if any test fails. HTTP tests start servers on free ports chosen at run time.
- Several classes have package-private test hooks, such as `FileReconstructor.openChunk`, `StorageNode.openForReading`, `StorageNode.deleteChunkFile`, `ManifestStore.writeContent` and `ManifestStore.moveIntoPlace`, `FileApiHandler.respondHealth` and `FileApiHandler.prepareDownload`, and `DistributedFsDemo.createNode`. Tests in the same package override them to simulate disk failures, corrupted data and crashes.
- Before suggesting a change, rebuild and run all 36 suites (629 normal tests) and, separately, the 10 stress tests, using the commands in [Run the tests](#run-the-tests).
