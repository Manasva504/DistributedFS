<#
.SYNOPSIS
    Measures upload and download throughput, failure-detection time and repair time on a local 3-node cluster, and writes docs\BENCHMARKS.md.

.DESCRIPTION
    Builds src/main/java into a new temporary folder, then makes -Runs runs. Each run starts a
    fresh cluster of three storage node processes and the server (replication factor 2, all on
    127.0.0.1, as scripts\demo.ps1 does), and:

      1. uploads and downloads a 1 MiB file once to warm up (not measured);
      2. times POST /files of a random -SizeMiB MiB file (the file is read into memory first, so
         only the request is timed: until the server's 201, which comes after both copies of
         every chunk and the manifest are stored);
      3. times GET /files/<id> of that file into a new file (the server checks every chunk and
         the whole file before it sends the first byte), and checks its SHA-256;
      4. kills node 1 and polls GET /cluster/status every 100 ms until node 1 is DOWN, timed from
         the moment the node's process has exited;
      5. times POST /cluster/repair (which answers when the repair has finished), then checks in
         the node folders that every chunk has a copy on node 0 and on node 2 again.

    A run whose download differs from the upload, whose node never goes DOWN, or whose repair
    does not restore 2 copies stops the benchmark with exit code 1, and nothing is written.
    Prints the CPU, RAM, OS and JDK, every run's numbers and the median of each, and writes them
    to -OutFile (UTF-8 without a BOM). Every run's folder is deleted afterwards.

    Not part of scripts\test.ps1 or CI: the numbers depend on the machine and on what else it is
    doing. Works in Windows PowerShell 5.1 and PowerShell 7.

.EXAMPLE
    .\scripts\bench.ps1
    .\scripts\bench.ps1 -Runs 5 -SizeMiB 32 -OutFile C:\temp\bench.md
#>
param(
    # How many times to repeat the whole measurement.
    [ValidateRange(1, 21)][int]$Runs = 3,
    # The size of the measured file, in MiB (the server accepts up to 100 by default).
    [ValidateRange(1, 100)][int]$SizeMiB = 64,
    # Where to write the results; docs\BENCHMARKS.md by default.
    [string]$OutFile
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'cluster-common.ps1')
if (-not $OutFile) { $OutFile = Join-Path $root 'docs\BENCHMARKS.md' }
$java = $null
$started = @()
$work = $null
$classes = $null

function Get-Median([double[]]$values) {
    $sorted = @($values | Sort-Object)
    $middle = [int][Math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2 -eq 1) { return $sorted[$middle] }
    return ($sorted[$middle - 1] + $sorted[$middle]) / 2
}

# The machine, the OS and the JDK, as label/value pairs.
function Get-SystemInfo {
    $cpu = @(Get-CimInstance Win32_Processor)
    $computer = Get-CimInstance Win32_ComputerSystem
    $os = Get-CimInstance Win32_OperatingSystem
    $javaVersion = @(& cmd.exe /c "`"$script:java`" -version 2>&1") -join '; '
    return [ordered]@{
        'CPU'  = ('{0} ({1} cores, {2} logical processors)' -f $cpu[0].Name.Trim(),
                  ($cpu | Measure-Object NumberOfCores -Sum).Sum, ($cpu | Measure-Object NumberOfLogicalProcessors -Sum).Sum)
        'RAM'  = ('{0:N1} GiB' -f ($computer.TotalPhysicalMemory / 1GB))
        'OS'   = ('{0} {1} (build {2})' -f $os.Caption.Trim(), $os.Version, $os.BuildNumber)
        'JDK'  = $javaVersion
        'PowerShell' = $PSVersionTable.PSVersion.ToString()
    }
}

# The source the benchmark was built from: the commit, and whether the working tree had changes.
function Get-SourceVersion {
    try {
        $describe = & git -C $root describe --always --dirty 2>$null
        if ($LASTEXITCODE -eq 0 -and $describe) { return "$describe" }
    } catch { }
    return 'unknown (git not available)'
}

# GET /cluster/status as an object.
function Get-Status([int]$port) {
    return ((New-Client).DownloadString("http://127.0.0.1:$port/cluster/status") | ConvertFrom-Json)
}

# One full run on a fresh cluster in its own folder. Returns its measurements.
function Invoke-BenchRun([int]$number) {
    $script:work = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-bench-" + [guid]::NewGuid().ToString('N'))
    $script:started = @()
    New-Item -ItemType Directory -Path $script:work | Out-Null
    Write-Host "== Run ${number}: work folder $script:work"
    try {
        $cluster = Start-Cluster 3
        $nodes = $cluster.Nodes
        $port = $cluster.Port
        $client = New-Client

        # 1. Warm-up, not measured.
        $warm = Join-Path $script:work 'warm-up.bin'
        [void](New-RandomFile $warm 1)
        $client.Headers.Add('Content-Type', 'application/octet-stream')
        $warmReply = [System.Text.Encoding]::UTF8.GetString(
            $client.UploadData("http://127.0.0.1:$port/files", 'POST', [System.IO.File]::ReadAllBytes($warm))) | ConvertFrom-Json
        [void]$client.DownloadData("http://127.0.0.1:$port/files/$($warmReply.fileId)")

        # 2. Upload.
        $file = Join-Path $script:work 'file.bin'
        $bytes = New-RandomFile $file $SizeMiB
        $client = New-Client
        $client.Headers.Add('Content-Type', 'application/octet-stream')
        $watch = [System.Diagnostics.Stopwatch]::StartNew()
        $replyBytes = $client.UploadData("http://127.0.0.1:$port/files", 'POST', $bytes)
        $watch.Stop()
        $uploadSeconds = $watch.Elapsed.TotalSeconds
        $f = [System.Text.Encoding]::UTF8.GetString($replyBytes) | ConvertFrom-Json
        $bytes = $null
        Write-Host ("   upload of {0} MiB: {1:N2} s, {2:N1} MiB/s ({3} chunks)" -f $SizeMiB, $uploadSeconds, ($SizeMiB / $uploadSeconds), $f.chunkCount)

        # 3. Download, then the SHA-256 check.
        $download = Join-Path $script:work 'file.download.bin'
        $watch = [System.Diagnostics.Stopwatch]::StartNew()
        (New-Client).DownloadFile("http://127.0.0.1:$port/files/$($f.fileId)", $download)
        $watch.Stop()
        $downloadSeconds = $watch.Elapsed.TotalSeconds
        Write-Host ("   download: {0:N2} s, {1:N1} MiB/s" -f $downloadSeconds, ($SizeMiB / $downloadSeconds))
        if (-not (Compare-Hashes $file $download $f.sha256)) { throw "run ${number}: the downloaded file differs from the upload" }

        # 4. Kill node 1; time until the status shows it DOWN.
        Stop-StartedProcess $nodes[1].Process
        if (-not $nodes[1].Process.HasExited) { throw "run ${number}: node 1 did not exit" }
        $watch = [System.Diagnostics.Stopwatch]::StartNew()
        $down = $false
        while ($watch.Elapsed.TotalSeconds -lt 30) {
            if ((Get-Status $port).nodes[1].state -eq 'DOWN') { $down = $true; break }
            Start-Sleep -Milliseconds 100
        }
        $watch.Stop()
        if (-not $down) { throw "run ${number}: node 1 was not DOWN within 30 s" }
        $detectSeconds = $watch.Elapsed.TotalSeconds
        Write-Host ("   node 1 DOWN {0:N2} s after its process exited" -f $detectSeconds)

        # 5. Repair; then every chunk must have a copy on node 0 and on node 2.
        $watch = [System.Diagnostics.Stopwatch]::StartNew()
        $reply = (New-Client).UploadString("http://127.0.0.1:$port/cluster/repair", 'POST', '')
        $watch.Stop()
        $repairSeconds = $watch.Elapsed.TotalSeconds
        $report = $reply | ConvertFrom-Json
        $placement = Get-ChunkPlacement $nodes $f.fileId $f.chunkCount
        foreach ($holders in $placement) {
            if ((@($holders | Where-Object { $_ -ne 1 }) -join ',') -ne '0,2') {
                throw "run ${number}: after the repair a chunk is on nodes $($holders -join ', ') only"
            }
        }
        if ($report.chunksLost -ne 0 -or $report.chunksNotRepaired -ne 0 -or $report.chunksUncertain -ne 0) {
            throw "run ${number}: the repair did not restore every chunk: $reply"
        }
        Write-Host ("   repair: {0:N2} s, {1} copies added (server's durationMillis {2}); every chunk on nodes 0 and 2" -f
            $repairSeconds, $report.copiesAdded, $report.durationMillis)

        return [pscustomobject]@{
            Run = $number; Chunks = $f.chunkCount
            UploadSeconds = $uploadSeconds; DownloadSeconds = $downloadSeconds
            UploadMiBps = $SizeMiB / $uploadSeconds; DownloadMiBps = $SizeMiB / $downloadSeconds
            DetectSeconds = $detectSeconds; RepairSeconds = $repairSeconds
            CopiesAdded = [int]$report.copiesAdded; RepairServerMillis = [int]$report.durationMillis
        }
    } finally {
        if (-not (Stop-AndCleanUp $false)) { Write-Host "   (the run's folder could not be deleted: $script:work)" }
    }
}

$exitCode = 1
$buildFolder = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-bench-build-" + [guid]::NewGuid().ToString('N'))
try {
    # Build once, into a folder of its own that outlives the runs' folders.
    $work = $buildFolder
    $classes = Join-Path $buildFolder 'classes'
    New-Item -ItemType Directory -Path $buildFolder | Out-Null
    Build-Classes $root
    $info = Get-SystemInfo
    $source = Get-SourceVersion
    $date = (Get-Date).ToString('yyyy-MM-dd HH:mm zzz')
    Write-Host "== System"
    foreach ($key in $info.Keys) { Write-Host ("   {0,-11} {1}" -f "${key}:", $info[$key]) }
    Write-Host "   Source:     $source"
    Write-Host "   Date:       $date"

    $results = @()
    for ($run = 1; $run -le $Runs; $run++) {
        $results += Invoke-BenchRun $run
    }

    $medians = [ordered]@{
        'Upload throughput'   = ('{0:N1} MiB/s ({1:N2} s)' -f (Get-Median ($results | ForEach-Object { $_.UploadMiBps })), (Get-Median ($results | ForEach-Object { $_.UploadSeconds })))
        'Download throughput' = ('{0:N1} MiB/s ({1:N2} s)' -f (Get-Median ($results | ForEach-Object { $_.DownloadMiBps })), (Get-Median ($results | ForEach-Object { $_.DownloadSeconds })))
        'Kill to DOWN'        = ('{0:N2} s' -f (Get-Median ($results | ForEach-Object { $_.DetectSeconds })))
        'Repair to 2 copies'  = ('{0:N2} s ({1} copies added)' -f (Get-Median ($results | ForEach-Object { $_.RepairSeconds })), (Get-Median ($results | ForEach-Object { $_.CopiesAdded })))
    }
    Write-Host "== Medians of $Runs runs ($SizeMiB MiB file, 3 nodes, 2 copies per chunk, 127.0.0.1)"
    foreach ($key in $medians.Keys) { Write-Host ("   {0,-20} {1}" -f "${key}:", $medians[$key]) }

    $md = New-Object System.Collections.Generic.List[string]
    $md.Add('# DistributedFS: benchmarks')
    $md.Add('')
    $md.Add("Written by ``scripts\bench.ps1`` on $date. These numbers describe one machine on one day; they are not run in CI and not a guarantee for other environments. See the [README](../README.md) for the overview.")
    $md.Add('')
    $md.Add('## Setup')
    $md.Add('')
    $md.Add("- **Cluster:** 3 storage node processes and the server, all on ``127.0.0.1``, replication factor 2, default 1 MiB chunks. Each run starts a fresh cluster in a new temporary folder.")
    $md.Add("- **File:** $SizeMiB MiB of random bytes, uploaded with ``POST /files`` and downloaded with ``GET /files/<id>``; the download's SHA-256 is checked against the upload. A 1 MiB upload and download before each run warm the JVMs up and are not measured.")
    $md.Add('- **Upload time:** from sending the request until the `201` answer, which comes after both copies of every chunk and the manifest are stored. The file is read into memory first, so reading it is not counted.')
    $md.Add('- **Download time:** from the request until the whole file is on disk. The server checks every chunk and the whole file before it sends the first byte, so this includes that check.')
    $md.Add('- **Kill to DOWN:** node 1 is killed; the clock starts when its process has exited, and `GET /cluster/status` is polled every 100 ms until it shows node 1 DOWN. The heartbeat checks every 2 seconds and needs 3 failed checks in a row, so this is expected to be roughly 4 to 6 seconds, depending on where in the 2-second cycle the kill falls.')
    $md.Add('- **Repair to 2 copies:** the time of `POST /cluster/repair` while node 1 is DOWN; the request answers when the repair has finished. Afterwards every chunk is checked to have a copy on node 0 and on node 2.')
    $md.Add("- **Runs:** $Runs; the tables give each run and the median.")
    $md.Add('')
    $md.Add('## System')
    $md.Add('')
    $md.Add('| | |')
    $md.Add('|---|---|')
    foreach ($key in $info.Keys) { $md.Add("| $key | $($info[$key]) |") }
    $md.Add("| Source | ``$source`` |")
    $md.Add('')
    $md.Add("## Medians of $Runs runs")
    $md.Add('')
    $md.Add('| Measure | Median |')
    $md.Add('|---|---|')
    foreach ($key in $medians.Keys) { $md.Add("| $key | $($medians[$key]) |") }
    $md.Add('')
    $md.Add('## Every run')
    $md.Add('')
    $md.Add('| Run | Upload | Download | Kill to DOWN | Repair | Copies added | Repair, server''s `durationMillis` |')
    $md.Add('|---|---|---|---|---|---|---|')
    foreach ($r in $results) {
        $md.Add(('| {0} | {1:N2} s, {2:N1} MiB/s | {3:N2} s, {4:N1} MiB/s | {5:N2} s | {6:N2} s | {7} | {8} ms |' -f $r.Run,
            $r.UploadSeconds, $r.UploadMiBps, $r.DownloadSeconds, $r.DownloadMiBps, $r.DetectSeconds, $r.RepairSeconds,
            $r.CopiesAdded, $r.RepairServerMillis))
    }
    $md.Add('')
    $md.Add('## Caveats')
    $md.Add('')
    $md.Add('- Everything runs on one machine over loopback, so these are not network numbers: there is no real network latency or bandwidth limit, and the nodes share the CPU and the disk.')
    $md.Add('- The operating system''s file cache holds recently written chunks, so reads may not touch the disk.')
    $md.Add('- The client is PowerShell''s `WebClient`, and its own overhead is included.')
    $md.Add('- Kill-to-DOWN is limited by the 100 ms polling and by the heartbeat''s fixed 2-second interval.')
    [System.IO.File]::WriteAllText($OutFile, (($md -join "`n") + "`n"), (New-Object System.Text.UTF8Encoding($false)))
    Write-Host "== Wrote $OutFile"
    $exitCode = 0
} catch {
    Write-Host "== BENCHMARK FAILED: $($_.Exception.Message)"
} finally {
    $work = $buildFolder
    $started = @()
    if (-not (Stop-AndCleanUp $false)) { $exitCode = 1 }
}
exit $exitCode
