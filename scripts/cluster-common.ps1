<#
.SYNOPSIS
    Shared helpers of scripts\demo.ps1, scripts\kill-demo.ps1 and scripts\repair-demo.ps1; dot-source it, do not run it.

.DESCRIPTION
    The caller sets $work (its own new temporary folder), $classes (a folder inside it) and
    $started = @() before using these functions; they record every process they start in
    $script:started, so the caller can stop exactly those processes, and nothing else, at the end.

    Processes are started with the JDK's own java.exe (found through java.home), not a launcher
    stub such as Oracle's javapath\java.exe, which runs the real JVM as a child: stopping the stub
    would leave the JVM running. Stopping a process also stops its children, in case a stub was
    used after all.

    Works in Windows PowerShell 5.1 and PowerShell 7. Never runs git.
#>

# The JDK's home folder. cmd.exe merges the -XshowSettings output (written to stderr) into stdout,
# so PowerShell 5.1 does not treat it as an error.
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

# Finds the JDK, prints it, and compiles src\main\java into $classes, the same way as scripts\test.ps1
# (an argument file instead of a wildcard). Sets $script:java.
function Build-Classes([string]$root) {
    $javaHome = Find-JavaHome
    $script:java = Join-Path $javaHome 'bin\java.exe'
    $javac = Join-Path $javaHome 'bin\javac.exe'
    Write-Host "== JDK: $javaHome"
    $sources = Get-ChildItem -Path (Join-Path $root 'src\main\java') -Recurse -Filter *.java |
        ForEach-Object { '"' + ($_.FullName -replace '\\', '/') + '"' }
    $listFile = Join-Path $work 'sources.txt'
    [System.IO.File]::WriteAllLines($listFile, [string[]]$sources)
    Write-Host "== Build ($(& $javac -version))"
    & $javac -encoding UTF-8 -d $classes "@$listFile"
    if ($LASTEXITCODE -ne 0) { throw "compilation failed (exit $LASTEXITCODE)" }
}

# Starts a Java program with its output in two log files, and waits until its output matches $ready.
function Start-JavaProcess([string]$name, [string[]]$javaArgs, [string]$ready) {
    $outLog = Join-Path $work "$name.out.log"
    $errLog = Join-Path $work "$name.err.log"
    # Start-Process joins its arguments with spaces, so each one is quoted.
    $quoted = @('-cp', $classes) + $javaArgs | ForEach-Object { '"' + $_ + '"' }
    $process = Start-Process -FilePath $script:java -ArgumentList $quoted -NoNewWindow -PassThru `
        -RedirectStandardOutput $outLog -RedirectStandardError $errLog
    $script:started += $process
    $deadline = (Get-Date).AddSeconds(30)
    while ($true) {
        $text = Read-Shared $outLog
        $match = [regex]::Match($text, $ready)
        if ($match.Success) {
            return @{ Process = $process; Match = $match; Log = $outLog; ErrLog = $errLog }
        }
        if ($process.HasExited -or (Get-Date) -gt $deadline) {
            throw "$name did not start. Its output:`n$text`n$(Read-Shared $errLog)"
        }
        Start-Sleep -Milliseconds 100
    }
}

# Starts $count storage node processes and the server with one --node option per node, all on
# free ports read from their ready lines, and prints them. Returns @{ Nodes; Server; Port }.
function Start-Cluster([int]$count) {
    $nodes = @()
    for ($i = 0; $i -lt $count; $i++) {
        $node = Start-JavaProcess "node-$i" @('com.distributedfs.StorageNodeServer', '--data-dir', (Join-Path $work "node-$i"),
            '--port', '0') 'DistributedFS storage node ([0-9a-f]{32}) listening on http://127\.0\.0\.1:(\d+)/'
        $nodes += @{ Id = $node.Match.Groups[1].Value; Port = [int]$node.Match.Groups[2].Value
                     Folder = (Join-Path $work "node-$i"); Process = $node.Process }
        Write-Host "== Node ${i}: $($nodes[$i].Id) on 127.0.0.1:$($nodes[$i].Port) (pid $($node.Process.Id))"
    }
    $serverArgs = @('com.distributedfs.DistributedFsServer', '--data-dir', (Join-Path $work 'server-data'), '--port', '0')
    foreach ($node in $nodes) { $serverArgs += @('--node', "127.0.0.1:$($node.Port)") }
    $server = Start-JavaProcess 'server' $serverArgs 'DistributedFS server listening on http://127\.0\.0\.1:(\d+)/'
    $port = [int]$server.Match.Groups[1].Value
    Write-Host "== Server on 127.0.0.1:$port (pid $($server.Process.Id)); its startup output:"
    (Read-Shared $server.Log).Trim() -split "`r?`n" | ForEach-Object { Write-Host "   $_" }
    return @{ Nodes = $nodes; Server = $server; Port = $port }
}

# A WebClient that goes straight to 127.0.0.1: no proxy and no "Expect: 100-continue".
function New-Client {
    [System.Net.ServicePointManager]::Expect100Continue = $false
    $client = New-Object System.Net.WebClient
    $client.Proxy = $null
    return $client
}

# Writes $mib MiB of random bytes to a new file and returns them.
function New-RandomFile([string]$path, [int]$mib) {
    $bytes = New-Object byte[] ($mib * 1024 * 1024)
    (New-Object System.Random).NextBytes($bytes)
    [System.IO.File]::WriteAllBytes($path, $bytes)
    return , $bytes
}

# POST /files with the file's bytes; prints and returns the server's JSON answer as an object.
function Invoke-Upload([int]$port, [string]$path) {
    $bytes = [System.IO.File]::ReadAllBytes($path)
    $client = New-Client
    $client.Headers.Add('Content-Type', 'application/octet-stream')
    Write-Host "== Upload: POST /files with $($bytes.Length) bytes"
    $reply = [System.Text.Encoding]::UTF8.GetString($client.UploadData("http://127.0.0.1:$port/files", 'POST', $bytes))
    Write-Host "   $reply"
    return ($reply | ConvertFrom-Json)
}

# GET /files/<fileId> into a new file.
function Invoke-Download([int]$port, [string]$fileId, [string]$path) {
    Write-Host "== Download: GET /files/$fileId"
    (New-Client).DownloadFile("http://127.0.0.1:$port/files/$fileId", $path)
}

# Compares the SHA-256 of two files and the one the server recorded; prints them; returns $true if all match.
function Compare-Hashes([string]$uploaded, [string]$downloaded, [string]$recorded) {
    $uploadHash = (Get-FileHash -LiteralPath $uploaded -Algorithm SHA256).Hash
    $downloadHash = (Get-FileHash -LiteralPath $downloaded -Algorithm SHA256).Hash
    Write-Host "   SHA-256 of the uploaded file:   $uploadHash"
    Write-Host "   SHA-256 of the downloaded file: $downloadHash"
    Write-Host "   SHA-256 the server recorded:    $($recorded.ToUpperInvariant())"
    return ($uploadHash -eq $downloadHash -and $uploadHash -eq $recorded.ToUpperInvariant())
}

# For each chunk of a file, the indexes of the nodes whose chunk folder holds a copy.
function Get-ChunkPlacement($nodes, [string]$fileId, [int]$chunkCount) {
    $placement = @()
    for ($c = 0; $c -lt $chunkCount; $c++) {
        $holders = @()
        for ($i = 0; $i -lt $nodes.Count; $i++) {
            if (Test-Path -LiteralPath (Join-Path $nodes[$i].Folder "chunks\${fileId}_$c")) { $holders += $i }
        }
        $placement += , $holders
    }
    return , $placement
}

# Prints how many chunk copies each node's folder holds.
function Write-CopiesPerNode($nodes) {
    for ($i = 0; $i -lt $nodes.Count; $i++) {
        $count = @(Get-ChildItem -LiteralPath (Join-Path $nodes[$i].Folder 'chunks') -File).Count
        Write-Host "   node ${i} ($($nodes[$i].Id)): $count"
    }
}

# Stops one process this script started, and its children, and waits until it has exited.
function Stop-StartedProcess($process) {
    $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$($process.Id)" -ErrorAction SilentlyContinue)
    if (-not $process.HasExited) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
    foreach ($child in $children) {
        Stop-Process -Id $child.ProcessId -Force -ErrorAction SilentlyContinue
    }
    [void]$process.WaitForExit(10000)
}

# Stops every process this script started, then deletes the work folder unless $keep. Returns
# $false if the folder could not be deleted.
function Stop-AndCleanUp([bool]$keep) {
    foreach ($process in $script:started) {
        Stop-StartedProcess $process
    }
    Write-Host "== Stopped the $($script:started.Count) processes this script started"
    if ($keep) {
        Write-Host "== Kept the work folder: $work"
        return $true
    }
    if (Test-Path -LiteralPath $work) {
        # A killed process's files can stay locked for a moment on Windows.
        for ($attempt = 0; $attempt -lt 20 -and (Test-Path -LiteralPath $work); $attempt++) {
            Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
            if (Test-Path -LiteralPath $work) { Start-Sleep -Milliseconds 250 }
        }
        if (Test-Path -LiteralPath $work) {
            Write-Host "== Could not delete the work folder: $work"
            return $false
        }
        Write-Host "== Deleted the work folder"
    }
    return $true
}
