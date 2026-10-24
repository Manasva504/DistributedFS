<#
.SYNOPSIS
    Runs DistributedFS with three storage node processes: upload, download and verify a file.

.DESCRIPTION
    Builds src/main/java into a new temporary folder, starts three storage node processes
    (StorageNodeServer) and the DistributedFS server with one --node option per node, each on a
    free port that the script reads from the process's own ready line. It then uploads a random
    5 MiB file, downloads it again, compares the SHA-256 of both copies with Get-FileHash, and shows
    how many chunks each node holds.

    Everything lives in one new folder in the system temp folder (distributedfs-cluster-demo-<random>).
    At the end the script stops only the processes it started and deletes that folder, unless -Keep
    is given (the processes are stopped either way). The repository's out/ and out-test/ folders are
    never touched. Exit code 0 means the downloaded file is identical to the uploaded one.

    Works in Windows PowerShell 5.1 and PowerShell 7.

.EXAMPLE
    .\scripts\demo.ps1
    .\scripts\demo.ps1 -Keep
#>
param(
    # Keep the work folder (node folders, server data, logs and both copies of the file).
    [switch]$Keep
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
# A new folder with a random name, created by this run only, so deleting it can never remove anything else.
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-cluster-demo-" + [guid]::NewGuid().ToString('N'))
$classes = Join-Path $work 'classes'
$started = @()
$exitCode = 1
$java = $null

# The JDK's own java.exe, not a launcher stub such as Oracle's javapath\java.exe, which runs the
# real JVM as a child process: stopping the stub would leave the JVM running. cmd.exe merges the
# -XshowSettings output (written to stderr) into stdout, so PowerShell 5.1 does not treat it as an error.
function Find-JavaHome {
    $settings = & cmd.exe /c "java -XshowSettings:properties -version 2>&1"
    foreach ($line in $settings) {
        if ($line -match '^\s*java\.home = (.+)$') { return $Matches[1].Trim() }
    }
    throw "could not find java.home; is a JDK on the PATH?"
}

# Reads a file that another process may still be writing.
function Read-Shared([string]$path) {
    if (-not (Test-Path -LiteralPath $path)) { return '' }
    $stream = [System.IO.File]::Open($path, 'Open', 'Read', 'ReadWrite')
    try {
        $reader = New-Object System.IO.StreamReader($stream)
        return $reader.ReadToEnd()
    } finally {
        $stream.Dispose()
    }
}

# Starts a Java program with its output in two log files, and waits until its output matches $ready.
function Start-JavaProcess([string]$name, [string[]]$javaArgs, [string]$ready) {
    $outLog = Join-Path $work "$name.out.log"
    $errLog = Join-Path $work "$name.err.log"
    # Start-Process joins its arguments with spaces, so each one is quoted.
    $quoted = @('-cp', $classes) + $javaArgs | ForEach-Object { '"' + $_ + '"' }
    $process = Start-Process -FilePath $java -ArgumentList $quoted -NoNewWindow -PassThru `
        -RedirectStandardOutput $outLog -RedirectStandardError $errLog
    $script:started += $process
    $deadline = (Get-Date).AddSeconds(30)
    while ($true) {
        $text = Read-Shared $outLog
        $match = [regex]::Match($text, $ready)
        if ($match.Success) {
            return @{ Process = $process; Match = $match; Log = $outLog }
        }
        if ($process.HasExited -or (Get-Date) -gt $deadline) {
            throw "$name did not start. Its output:`n$text`n$(Read-Shared $errLog)"
        }
        Start-Sleep -Milliseconds 100
    }
}

try {
    New-Item -ItemType Directory -Path $work | Out-Null
    Write-Host "== Work folder: $work"
    $javaHome = Find-JavaHome
    $java = Join-Path $javaHome 'bin\java.exe'
    $javac = Join-Path $javaHome 'bin\javac.exe'
    Write-Host "== JDK: $javaHome"

    # The same build as scripts\test.ps1: an argument file instead of a wildcard.
    $sources = Get-ChildItem -Path (Join-Path $root 'src\main\java') -Recurse -Filter *.java |
        ForEach-Object { '"' + ($_.FullName -replace '\\', '/') + '"' }
    $listFile = Join-Path $work 'sources.txt'
    [System.IO.File]::WriteAllLines($listFile, [string[]]$sources)
    Write-Host "== Build ($(& $javac -version))"
    & $javac -encoding UTF-8 -d $classes "@$listFile"
    if ($LASTEXITCODE -ne 0) { throw "compilation failed (exit $LASTEXITCODE)" }

    $nodes = @()
    for ($i = 0; $i -lt 3; $i++) {
        $node = Start-JavaProcess "node-$i" @('com.distributedfs.StorageNodeServer', '--data-dir', (Join-Path $work "node-$i"),
            '--port', '0') 'DistributedFS storage node ([0-9a-f]{32}) listening on http://127\.0\.0\.1:(\d+)/'
        $nodes += @{ Id = $node.Match.Groups[1].Value; Port = [int]$node.Match.Groups[2].Value; Folder = (Join-Path $work "node-$i") }
        Write-Host "== Node ${i}: $($nodes[$i].Id) on 127.0.0.1:$($nodes[$i].Port) (pid $($node.Process.Id))"
    }

    $serverArgs = @('com.distributedfs.DistributedFsServer', '--data-dir', (Join-Path $work 'server-data'), '--port', '0')
    foreach ($node in $nodes) { $serverArgs += @('--node', "127.0.0.1:$($node.Port)") }
    $server = Start-JavaProcess 'server' $serverArgs 'DistributedFS server listening on http://127\.0\.0\.1:(\d+)/'
    $port = [int]$server.Match.Groups[1].Value
    Write-Host "== Server on 127.0.0.1:$port (pid $($server.Process.Id)); its startup output:"
    (Read-Shared $server.Log).Trim() -split "`r?`n" | ForEach-Object { Write-Host "   $_" }

    $uploadFile = Join-Path $work 'upload.bin'
    $downloadFile = Join-Path $work 'download.bin'
    $bytes = New-Object byte[] (5 * 1024 * 1024)
    (New-Object System.Random).NextBytes($bytes)
    [System.IO.File]::WriteAllBytes($uploadFile, $bytes)

    # No proxy and no "Expect: 100-continue": the request goes straight to 127.0.0.1.
    [System.Net.ServicePointManager]::Expect100Continue = $false
    $client = New-Object System.Net.WebClient
    $client.Proxy = $null
    $client.Headers.Add('Content-Type', 'application/octet-stream')
    Write-Host "== Upload: POST /files with $($bytes.Length) bytes"
    $reply = [System.Text.Encoding]::UTF8.GetString($client.UploadData("http://127.0.0.1:$port/files", 'POST', $bytes))
    Write-Host "   $reply"
    $manifest = $reply | ConvertFrom-Json
    Write-Host "== Download: GET /files/$($manifest.fileId)"
    $client.DownloadFile("http://127.0.0.1:$port/files/$($manifest.fileId)", $downloadFile)

    $uploadHash = (Get-FileHash -LiteralPath $uploadFile -Algorithm SHA256).Hash
    $downloadHash = (Get-FileHash -LiteralPath $downloadFile -Algorithm SHA256).Hash
    Write-Host "== SHA-256 of the uploaded file:   $uploadHash"
    Write-Host "   SHA-256 of the downloaded file: $downloadHash"
    Write-Host "   SHA-256 the server recorded:    $($manifest.sha256.ToUpperInvariant())"
    Write-Host "== Chunks per node ($($manifest.chunkCount) chunks of $($manifest.chunkSize) bytes):"
    for ($i = 0; $i -lt 3; $i++) {
        $count = @(Get-ChildItem -LiteralPath (Join-Path $nodes[$i].Folder 'chunks') -File).Count
        Write-Host "   node ${i} ($($nodes[$i].Id)): $count"
    }
    if ($uploadHash -eq $downloadHash -and $uploadHash -eq $manifest.sha256.ToUpperInvariant()) {
        Write-Host "== RESULT: SUCCESS (the downloaded file is identical to the uploaded one)"
        $exitCode = 0
    } else {
        Write-Host "== RESULT: FAILURE (the checksums differ)"
    }
} catch {
    Write-Host "== RESULT: FAILURE ($($_.Exception.Message))"
} finally {
    # Only the processes this script started, and any child process of theirs (in case a
    # launcher stub was used after all).
    foreach ($process in $started) {
        $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$($process.Id)" -ErrorAction SilentlyContinue)
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
        foreach ($child in $children) {
            Stop-Process -Id $child.ProcessId -Force -ErrorAction SilentlyContinue
        }
        [void]$process.WaitForExit(10000)
    }
    Write-Host "== Stopped the $($started.Count) processes this script started"
    if ($Keep) {
        Write-Host "== Kept the work folder: $work"
    } elseif (Test-Path -LiteralPath $work) {
        # A killed process's files can stay locked for a moment on Windows.
        for ($attempt = 0; $attempt -lt 20 -and (Test-Path -LiteralPath $work); $attempt++) {
            Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
            if (Test-Path -LiteralPath $work) { Start-Sleep -Milliseconds 250 }
        }
        if (Test-Path -LiteralPath $work) {
            Write-Host "== Could not delete the work folder: $work"
            $exitCode = 1
        } else {
            Write-Host "== Deleted the work folder"
        }
    }
}
exit $exitCode
