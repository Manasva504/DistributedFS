<#
.SYNOPSIS
    Shows failure detection and repair: a storage node dies, the heartbeat marks it DOWN, a repair restores 2 copies, the node comes back, and a second repair trims the extras.

.DESCRIPTION
    Builds src/main/java into a new temporary folder and starts three storage node processes and
    the DistributedFS server, as scripts\demo.ps1 does. Then:

      1. uploads a random 5 MiB file and shows on which nodes each chunk's 2 copies are;
      2. kills node 1's process and polls GET /cluster/status until node 1 is DOWN;
      3. runs POST /cluster/repair?dryRun=true (which must change nothing), then POST
         /cluster/repair, and checks that every chunk now has its 2 copies on nodes 0 and 2;
      4. restarts node 1 on its old folder and port, and polls the status until it is UP;
      5. runs a dry run, which must report the extra copies (copiesRemoved > 0), then a repair,
         and checks that every chunk has exactly 2 copies again;
      6. downloads the file and compares its SHA-256 with the original.

    Everything lives in one new folder in the system temp folder (distributedfs-cluster-demo-<random>).
    At the end the script stops only the processes it started and deletes that folder, unless -Keep
    is given. Exit code 0 means every check passed. The process handling is shared with
    scripts\demo.ps1 (scripts\cluster-common.ps1).

    Works in Windows PowerShell 5.1 and PowerShell 7.

.EXAMPLE
    .\scripts\repair-demo.ps1
    .\scripts\repair-demo.ps1 -Keep
#>
param(
    # Keep the work folder (node folders, server data, logs and the files).
    [switch]$Keep
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'cluster-common.ps1')
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-cluster-demo-" + [guid]::NewGuid().ToString('N'))
$classes = Join-Path $work 'classes'
$started = @()
$java = $null
$exitCode = 1

function Write-Placement($placement) {
    for ($c = 0; $c -lt $placement.Count; $c++) {
        Write-Host "   chunk ${c}: nodes $($placement[$c] -join ', ')"
    }
}

# GET /cluster/status as an object.
function Get-Status([int]$port) {
    return ((New-Client).DownloadString("http://127.0.0.1:$port/cluster/status") | ConvertFrom-Json)
}

function Write-Status($status) {
    foreach ($node in $status.nodes) {
        Write-Host ("   node {0} ({1}): {2}, last good check {3}, failures in a row {4}" -f $node.index, $node.address,
            $node.state, $(if ($node.lastGoodCheck) { $node.lastGoodCheck } else { 'never' }), $node.failuresInARow)
    }
}

# Polls the status every 500 ms until node $index is in $state; prints the status; returns $true if it got there in time.
function Wait-NodeState([int]$port, [int]$index, [string]$state) {
    Write-Host "== Polling GET /cluster/status until node $index is $state"
    $begin = Get-Date
    $deadline = $begin.AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $status = Get-Status $port
        if ($status.nodes[$index].state -eq $state) {
            Write-Host ("   node {0} is {1} after {2:N1} s:" -f $index, $state, ((Get-Date) - $begin).TotalSeconds)
            Write-Status $status
            return $true
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Host "   node $index did not become $state within 30 s:"
    Write-Status (Get-Status $port)
    return $false
}

# POST /cluster/repair, with ?dryRun=true if $dryRun; prints the JSON report and returns it as an object.
function Invoke-Repair([int]$port, [bool]$dryRun) {
    $url = "http://127.0.0.1:$port/cluster/repair$(if ($dryRun) { '?dryRun=true' })"
    Write-Host "== POST /cluster/repair$(if ($dryRun) { '?dryRun=true' })"
    $reply = (New-Client).UploadString($url, 'POST', '')
    Write-Host "   $reply"
    return ($reply | ConvertFrom-Json)
}

# Every node folder's chunk files with their sizes and times, to show that a dry run changed nothing.
function Get-NodeFiles($nodes) {
    $lines = @()
    for ($i = 0; $i -lt $nodes.Count; $i++) {
        foreach ($file in Get-ChildItem -LiteralPath (Join-Path $nodes[$i].Folder 'chunks') -File) {
            $lines += "$i/$($file.Name)/$($file.Length)/$($file.LastWriteTimeUtc.Ticks)"
        }
    }
    return ($lines | Sort-Object) -join "`n"
}

try {
    New-Item -ItemType Directory -Path $work | Out-Null
    Write-Host "== Work folder: $work"
    Build-Classes $root
    $cluster = Start-Cluster 3
    $nodes = $cluster.Nodes
    $port = $cluster.Port
    $checks = @()

    # 1. A file, with all three nodes up.
    $file = Join-Path $work 'file.bin'
    [void](New-RandomFile $file 5)
    $f = Invoke-Upload $port $file
    Write-Host "== Where the 2 copies of each chunk are:"
    Write-Placement (Get-ChunkPlacement $nodes $f.fileId $f.chunkCount)
    Write-Host "== GET /cluster/status:"
    Write-Status (Get-Status $port)

    # 2. Node 1 dies; the heartbeat notices.
    Write-Host "== Killing node 1 (pid $($nodes[1].Process.Id))"
    Stop-StartedProcess $nodes[1].Process
    Write-Host "   node 1 has exited: $($nodes[1].Process.HasExited)"
    $checks += Wait-NodeState $port 1 'DOWN'

    # 3. Dry run (changes nothing), then the repair: every chunk ends up on nodes 0 and 2.
    $before = Get-NodeFiles $nodes
    $dry = Invoke-Repair $port $true
    $unchanged = ((Get-NodeFiles $nodes) -eq $before)
    Write-Host "   the dry run changed no node folder: $unchanged"
    $checks += $unchanged
    $real = Invoke-Repair $port $false
    $checks += ($real.copiesAdded -eq $dry.copiesAdded -and $real.copiesAdded -gt 0 -and $real.chunksLost -eq 0)
    $placement = Get-ChunkPlacement $nodes $f.fileId $f.chunkCount
    Write-Host "== Where the copies are after the repair:"
    Write-Placement $placement
    $onZeroAndTwo = $true
    foreach ($holders in $placement) {
        if ((@($holders | Where-Object { $_ -ne 1 }) -join ',') -ne '0,2') { $onZeroAndTwo = $false }
    }
    Write-Host "   every chunk has a copy on node 0 and on node 2: $onZeroAndTwo"
    $checks += $onZeroAndTwo

    # 4. Node 1 comes back on its old folder and port.
    Write-Host "== Restarting node 1 on its folder and port $($nodes[1].Port)"
    $restarted = $null
    for ($attempt = 1; $attempt -le 5 -and -not $restarted; $attempt++) {
        try {
            $restarted = Start-JavaProcess "node-1-restarted-$attempt" @('com.distributedfs.StorageNodeServer', '--data-dir',
                $nodes[1].Folder, '--port', "$($nodes[1].Port)") 'DistributedFS storage node ([0-9a-f]{32}) listening on'
        } catch {
            Write-Host "   attempt ${attempt}: $($_.Exception.Message -split "`n" | Select-Object -Last 2)"
            Start-Sleep -Seconds 1
        }
    }
    if (-not $restarted) { throw "node 1 could not be restarted on port $($nodes[1].Port)" }
    $sameId = ($restarted.Match.Groups[1].Value -eq $nodes[1].Id)
    Write-Host "   node 1 is $($restarted.Match.Groups[1].Value) again (pid $($restarted.Process.Id)); same ID: $sameId"
    $nodes[1].Process = $restarted.Process
    $checks += $sameId
    $checks += Wait-NodeState $port 1 'UP'

    # 5. The chunks that were copied while node 1 was down now have 3 copies; the repair trims them.
    Write-Host "== Copies per node before trimming:"
    Write-CopiesPerNode $nodes
    $dryTrim = Invoke-Repair $port $true
    $checks += ($dryTrim.copiesRemoved -gt 0)
    $trim = Invoke-Repair $port $false
    $checks += ($trim.copiesRemoved -eq $dryTrim.copiesRemoved)
    $placement = Get-ChunkPlacement $nodes $f.fileId $f.chunkCount
    Write-Host "== Where the copies are after trimming:"
    Write-Placement $placement
    $exactlyTwo = $true
    foreach ($holders in $placement) {
        if (@($holders).Count -ne 2) { $exactlyTwo = $false }
    }
    Write-Host "   every chunk has exactly 2 copies: $exactlyTwo"
    $checks += $exactlyTwo

    # 6. The file is intact.
    $download = Join-Path $work 'file.download.bin'
    Invoke-Download $port $f.fileId $download
    Write-Host "== Checksums:"
    $checks += Compare-Hashes $file $download $f.sha256

    Write-Host "== The server's heartbeat and repair log lines:"
    foreach ($line in @((Read-Shared $cluster.Server.ErrLog) -split "`r?`n" | Where-Object { $_ -match 'DOWN|UP again|[Rr]epair' })) {
        Write-Host "   $line"
    }

    if (@($checks | Where-Object { -not $_ }).Count -eq 0) {
        Write-Host "== RESULT: SUCCESS (node 1 was detected DOWN, the repair restored 2 copies on nodes 0 and 2, node 1 came back UP, the extra copies were trimmed, and the file downloaded intact)"
        $exitCode = 0
    } else {
        Write-Host "== RESULT: FAILURE (checks: $($checks -join ', '))"
    }
} catch {
    Write-Host "== RESULT: FAILURE ($($_.Exception.Message))"
} finally {
    if (-not (Stop-AndCleanUp $Keep.IsPresent)) { $exitCode = 1 }
}
exit $exitCode
