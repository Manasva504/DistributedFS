# DistributedFS: testing

How the tests are built and run, what continuous integration checks, and the results recorded for each milestone. See the [README](../README.md) for the overview and [DESIGN.md](DESIGN.md) for the design.

## Run the tests

Each suite is a plain-Java program. It prints `PASS`, `FAIL` or `SKIP` for every test and ends with a summary such as `18 passed, 0 failed`. A suite exits with code 1 if any test fails.

| Suite | Normal tests | Stress tests (run separately) |
|---|---|---|
| `ApiErrorTest` | 5 | none |
| `ChecksumServiceTest` | 15 | 1 |
| `ClusterEventsTest` | 12 | none |
| `ClusterStatusEndpointTest` | 11 | none |
| `ConsoleAssetsTest` | 13 | none |
| `ConsoleEndpointTest` | 15 | none |
| `CrashRecoveryTest` | 8 | 1 |
| `CrossOriginTest` | 18 | none |
| `DataDirectoryTest` | 20 | none |
| `DegradedStartupTest` | 15 | none |
| `DistributedFsDemoTest` | 18 | 1 |
| `DistributedFsServerTest` | 28 | none |
| `DownloadEndpointTest` | 26 | 1 |
| `FileChunkerTest` | 18 | none |
| `FileIdsTest` | 12 | none |
| `FileNamesEndpointTest` | 10 | none |
| `FileNamesTest` | 14 | none |
| `FileReconstructorTest` | 15 | 1 |
| `FileServiceTest` | 34 | none |
| `FileTreesTest` | 5 | none |
| `FilesListEndpointTest` | 16 | none |
| `HeartbeatMonitorTest` | 14 | none |
| `JsonTest` | 8 | none |
| `ManifestStoreTest` | 28 | none |
| `NetworkedRecoveryTest` | 8 | none |
| `NodeApiTest` | 26 | none |
| `NodeDataDirectoryTest` | 11 | none |
| `PlacementEndpointTest` | 14 | none |
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
| **Total** | **741** | **10** |

The simplest way to build and run everything is the script that continuous integration uses. From the repository folder:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\test.ps1                 # build, then every normal suite
powershell -ExecutionPolicy Bypass -File scripts\test.ps1 -Stress         # build, then the stress tests with -Xmx16m
powershell -ExecutionPolicy Bypass -File scripts\test.ps1 -Suite StorageNodeTest,FileServiceTest
```

Each run compiles into a new temporary folder (never `out` or `out-test`), so classes left over from deleted or renamed sources cannot hide an error, and deletes that folder when it finishes. It prints every suite's output, then a total and the name of every skipped test, and exits with code 1 if compilation or any suite failed. It works in Windows PowerShell 5.1 and PowerShell 7. (`-ExecutionPolicy Bypass` applies to that one command only; it is needed on machines whose policy blocks local scripts.)

To run the suites by hand instead, from the repository folder:

```powershell
foreach ($suite in 'ApiErrorTest', 'ChecksumServiceTest', 'ClusterEventsTest', 'ClusterStatusEndpointTest', 'ConsoleAssetsTest',
        'ConsoleEndpointTest', 'CrashRecoveryTest', 'CrossOriginTest',
        'DataDirectoryTest', 'DegradedStartupTest', 'DistributedFsDemoTest', 'DistributedFsServerTest', 'DownloadEndpointTest', 'FileChunkerTest',
        'FileIdsTest', 'FileNamesEndpointTest', 'FileNamesTest', 'FileReconstructorTest', 'FileServiceTest', 'FileTreesTest', 'FilesListEndpointTest', 'HeartbeatMonitorTest', 'JsonTest',
        'ManifestStoreTest', 'NetworkedRecoveryTest', 'NodeApiTest', 'NodeDataDirectoryTest', 'PlacementEndpointTest', 'RemoteClusterTest',
        'RemoteServerTest', 'RemoteStorageNodeTest', 'RepairEndpointTest', 'RepairServiceTest',
        'ReplicatedClusterTest', 'ReplicatedRecoveryTest',
        'ReplicatedServerTest',
        'StaleTempFilesTest', 'StalledRequestTest', 'StallWatchTest', 'StartupRecoveryTest',
        'StorageClusterTest', 'StorageNodeServerTest', 'StorageNodeTest', 'UploadEndpointTest') {
    java -cp "out;out-test;src\main\resources" "com.distributedfs.$suite"
}
```

Or run one suite, for example:

```powershell
java -cp "out;out-test;src\main\resources" com.distributedfs.UploadEndpointTest
```

`src\main\resources` holds the web console's files, which the server reads from the class path; without it, `ConsoleEndpointTest` and `ConsoleAssetsTest` fail and the server prints a warning in the other suites.

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

## Checking the web console

The Java suites test the console's endpoints, headers and files, but not the page in a browser. `scripts\console-smoke.ps1` does that, by hand:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\console-smoke.ps1
```

It uses the Chrome or Edge already installed (standard paths; `-Browser <path>` for another one; nothing is downloaded), starts three nodes and the server, uploads two files, and then:

- checks the headers of `GET /` and the two asset paths (exact Content-Security-Policy, `nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store`, content types), and that a `POST /cluster/repair` with a foreign `Origin` is refused;
- loads the page in headless mode and checks the DOM after its scripts ran: they polled the API (`data-console-ready`), no JavaScript error or Content-Security-Policy violation was reported (the page counts both on `<body>`), and it shows "Healthy", the three node IDs, both files and chunk tiles;
- through the browser's DevTools protocol on a loopback port, shows the page at exactly 1440 x 900, 390 x 844 and 390 x 2400 CSS pixels (as a phone below 600), checks that it does not scroll sideways at each width, and saves a screenshot of each (headless Chrome never makes its window narrower than about 500 px, so `--window-size` alone cannot show a phone layout);
- measures the contrast of every visible run of text in each screenshot: the text's color from the page, its background from the screenshot's pixels around it (a frosted panel's background is whatever shows through it), against WCAG AA (4.5:1, or 3:1 for large text);
- through the page itself, gives its file input `report.pdf` and `résumé 2026.pdf` (DevTools `DOM.setFileInputFiles`, so the page's own upload code runs), checks that the server stored each under its name and names it in `Content-Disposition`, clicks each file's download link on the page, and checks that Chrome saves it under exactly the same name with the same SHA-256, and that nothing else is left in the download folder: nobody has to rename a file;
- kills node 1, waits until it is DOWN, and repeats the DOM and screenshot checks for the degraded cluster ("Degraded", node 1 DOWN, its copies unreachable).

The screenshots go to a new `distributedfs-console-shots-<time>` folder in the temp folder (or `-OutDir`), and the script exits with code 0 only if every check passed. It is not part of CI: it depends on a browser outside the JDK, whose version on hosted runners changes without notice, and on headless timing. Once it has run reliably by hand for a while, it could become a CI step that does not fail the build.

Checked by hand, not by a script (see the recorded runs):

- `scripts\console-demo.ps1`: kill node 1 (`k`) and watch it turn DOWN; Dry run, then Repair, and see the new copies; restart it (`r`), then Repair again to trim the extra copies;
- drag a file onto the page and choose one with the button; download a file and compare its SHA-256;
- hide the tab and see in the browser's Network tab that polling stops, and starts again when the tab is shown;
- use the page with the keyboard only, including opening and closing the repair report (Esc);
- narrow the window to phone width.

**Done by hand on 31 October 2026,** by the project's owner, in a real browser window with `scripts\console-demo.ps1`: every step above worked, including the Dry run and Repair buttons and their report, uploads by drag and drop and with the button, and a real PDF that downloaded under its own name and opened directly.

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

A run on 28 October 2026, on the same machine, tested milestone M4b-1 (the cross-site check on the server and the storage nodes) before it was committed:

- **Normal tests:** `scripts\test.ps1` in Windows PowerShell 5.1: all 37 suites exited with code 0. Of the 647 normal tests, 647 passed, 0 failed and 0 were skipped. The run took about 123 seconds.
- **Stress tests,** run separately with `scripts\test.ps1 -Stress`: all 10 passed with `-Xmx16m`, in about 17 seconds.
- **Deliberate-break checks:** 7 checks each broke one rule of the cross-site check in a separate copy of the source, and `CrossOriginTest` detected all 7: the check skipped for `/cluster/repair`; `Origin` compared as a prefix; `Origin: null` accepted; an origin accepted for either `127.0.0.1` or `localhost` whatever the request's Host; `Sec-Fetch-Site` ignored; the check applied to `POST /files` only; the storage node's check removed.
- **`scripts\demo.ps1`**, **`scripts\kill-demo.ps1`** and **`scripts\repair-demo.ps1`:** all three printed `RESULT: SUCCESS` and exited with code 0, and none left a process or folder behind.

A run on 29 October 2026, on the same machine, tested milestone M4b-2 (the web console and its three endpoints) before it was committed:

- **Normal tests:** `scripts\test.ps1` in Windows PowerShell 5.1: all 42 suites exited with code 0. Of the 716 normal tests, 716 passed, 0 failed and 0 were skipped. The run took about 148 seconds. Three assertions of earlier suites were changed on purpose: `/` is no longer an unknown path (`DistributedFsServerTest`), and `GET /files` is no longer `405`, so the `Allow` header of `/files` is `GET, POST` (`UploadEndpointTest`, twice).
- **Stress tests,** run separately with `scripts\test.ps1 -Stress`: all 10 passed with `-Xmx16m`, in about 17 seconds.
- **Deliberate-break checks:** 18 checks (RB61 to RB78) each broke one rule in a separate copy of the source, and the tests detected all 18: the Content-Security-Policy header missing; `'unsafe-inline'` allowed; `nosniff` missing on the console's files; `frame-ancestors` missing; console files served by name from the resources folder; `HEAD` served; `innerHTML` used in the script; `GET /files` under the storage lock; the node listing reading chunk bytes; a DOWN node's copies reported reachable; a committed file with a leftover marker hidden; the event list unbounded; the listing not refreshed after a repair; not refreshed after an upload; trim candidates marked while a node is DOWN; a DOWN event recorded on every failed check; a query accepted on a console path; a placement's file ID decoded before it is checked. One needed a better test first: the listing not refreshed after an upload was detected by `FilesListEndpointTest` but not by `PlacementEndpointTest`, whose test made its first request for an unknown file, which lists nothing; that test now asks for a real file first, and detects it.
- **`scripts\console-smoke.ps1`** with Chrome 156: all 43 checks passed, in about 37 seconds: the headers, the cross-site refusal, the page's scripts ran with no JavaScript error and no Content-Security-Policy violation, the healthy and the degraded cluster were shown as such, the page did not scroll sideways at 1440, 390 (844 and 2400 high) CSS pixels, and every measured run of text passed WCAG AA in all five screenshots (the lowest: the large, bold "Repair" label, 3.26:1 where 3:1 is needed). Earlier runs had found two problems: with node 1 DOWN, the chunk map's wider column headers made the page 414 px wide at 390, so the map's panel now clips its own scrolling; and once the color shapes showed through the panels, red text on the pink and peach tints measured 4.26 to 4.46:1, so red text now uses `#B42F2F` (the status red `#C93A3A` stays for dots, borders and tints).
- **`scripts\console-demo.ps1`,** with the keys given through a pipe (`k`, an unknown key, `r`, `q`) and `-NoBrowser`: it killed node 1, restarted it on its port, stopped all 5 processes and deleted its folder, exit code 0.
- **`scripts\demo.ps1`**, **`scripts\kill-demo.ps1`** and **`scripts\repair-demo.ps1`:** all three printed `RESULT: SUCCESS` and exited with code 0, and none left a process or folder behind.
- **Not checked yet:** the hand checks listed in [Checking the web console](#checking-the-web-console) (drag and drop, a download from the page, polling stopping while the tab is hidden, keyboard use, and the Repair and Dry run buttons in a real browser window). The buttons, uploads and downloads go through the same endpoints the Java suites test, but the page's own handling of them has only been run in headless Chrome, without clicks.

A later run on 29 October 2026, still before commit, tested original file names (`X-File-Name`, manifest format 3, `Content-Disposition`), added to M4b-2:

- **Normal tests:** all 44 suites exited with code 0. Of the 741 normal tests, 741 passed, 0 failed and 0 were skipped, in about 150 seconds. Four assertions of earlier suites changed on purpose, because every answer that describes a file now has a `name` field (a string, or `null`): the exact `201` body in `UploadEndpointTest`, the `upload_finished` event in `ClusterEventsTest`, and the field lists in `FilesListEndpointTest` and `PlacementEndpointTest`. `UploadEndpointTest`'s storage-failure hook now overrides the `FileService.store` method that takes a name, which the handler calls.
- **Stress tests:** all 10 passed with `-Xmx16m`, in about 16 seconds.
- **Deliberate-break checks:** 15 checks (RB79 to RB93) each broke one rule in a separate copy of the source, and the tests detected all 15: the download's ASCII fallback keeping `"` (a second `filename` parameter); the name written into `filename*` unencoded; `/` accepted; 256 code points accepted; `.` and `..` accepted; malformed UTF-8 replaced instead of refused; format 2 manifests required to have a name; the name written into the manifest as raw UTF-8; `filename*` leaving spaces, quotes and parentheses unencoded; bidi overrides accepted; a file without a name downloaded without `<fileId>.bin`; the raw header stored instead of the decoded name; the console inserting a name as HTML; names normalized to NFC; the console not sending `X-File-Name`. One needed a better test first: the fallback keeping `"` was detected by `FileNamesTest` but not by `FileNamesEndpointTest`, which compared the header only with what `FileNames` builds; that test now also checks the header's structure on its own (two quotes, one `filename`, one `filename*`).
- **`scripts\console-smoke.ps1`:** all 52 checks passed, in about 47 seconds, including the new ones: through the page's own file input and download links, `report.pdf` and `résumé 2026.pdf` were uploaded under their names and saved by Chrome under exactly the same names with the same SHA-256, and nothing else was left in the download folder.
- **`scripts\demo.ps1`**, **`scripts\kill-demo.ps1`** and **`scripts\repair-demo.ps1`:** all three printed `RESULT: SUCCESS` and exited with code 0. **`scripts\console-demo.ps1`,** with the keys given through a pipe and `-NoBrowser`, killed and restarted node 1, stopped all 5 processes and deleted its folder, exit code 0 (it prints no `RESULT` line: it waits for keys).
- **By hand, on 31 October 2026:** the owner clicked through every step of the [checklist](#checking-the-web-console) in a real browser, and all of them worked, including a real PDF that downloaded under its own name and opened directly. This closes the hand checks listed as not yet done in the run above.

## Running tests and contributing

Personal learning project; issues and suggestions welcome.

- Each `*Test` class is a small test runner with its own `main` method; no test framework is used. Every test that touches files runs in its own temporary folder, which is deleted afterwards, and a suite exits with code 1 if any test fails. HTTP tests start servers on free ports chosen at run time.
- Several classes have package-private test hooks, such as `FileReconstructor.openChunk`, `StorageNode.openForReading`, `StorageNode.deleteChunkFile`, `ManifestStore.writeContent` and `ManifestStore.moveIntoPlace`, `FileApiHandler.respondHealth` and `FileApiHandler.prepareDownload`, and `DistributedFsDemo.createNode`. Tests in the same package override them to simulate disk failures, corrupted data and crashes.
- Before suggesting a change, rebuild and run all 44 suites (741 normal tests) and, separately, the 10 stress tests, using the commands in [Run the tests](#run-the-tests).
