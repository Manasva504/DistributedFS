<#
.SYNOPSIS
    Shows replication at work: a storage node process is killed, and files can still be downloaded and uploaded.

.DESCRIPTION
    Builds src/main/java into a new temporary folder and starts three storage node processes and
    the DistributedFS server, as scripts\demo.ps1 does. Then:

      1. uploads a random 5 MiB file A and shows on which nodes each chunk's 2 copies are;
      2. kills node 1's process and waits until it has exited;
      3. downloads A and compares its SHA-256 with the original;
      4. uploads a new random 5 MiB file B while node 1 is still dead, and checks that every chunk
         of B got its 2 copies on nodes 0 and 2;
      5. downloads B and compares its SHA-256 with the original;
      6. prints the server's log lines that show node 1 being skipped.

    Everything lives in one new folder in the system temp folder (distributedfs-cluster-demo-<random>).
    At the end the script stops only the processes it started and deletes that folder, unless -Keep
    is given. Exit code 0 means every check passed. The process handling is shared with
    scripts\demo.ps1 (scripts\cluster-common.ps1).

    Works in Windows PowerShell 5.1 and PowerShell 7.

.EXAMPLE
    .\scripts\kill-demo.ps1
    .\scripts\kill-demo.ps1 -Keep
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

try {
    New-Item -ItemType Directory -Path $work | Out-Null
    Write-Host "== Work folder: $work"
    Build-Classes $root
    $cluster = Start-Cluster 3
    $nodes = $cluster.Nodes
    $port = $cluster.Port
    $checks = @()

    # 1. File A, with all three nodes up.
    Write-Host "== File A"
    $fileA = Join-Path $work 'a.bin'
    [void](New-RandomFile $fileA 5)
    $a = Invoke-Upload $port $fileA
    Write-Host "== Where the 2 copies of each chunk of A are:"
    Write-Placement (Get-ChunkPlacement $nodes $a.fileId $a.chunkCount)

    # 2. Node 1 dies.
    Write-Host "== Killing node 1 (pid $($nodes[1].Process.Id))"
    Stop-StartedProcess $nodes[1].Process
    Write-Host "   node 1 has exited: $($nodes[1].Process.HasExited)"

    # 3. A is still readable.
    $downloadA = Join-Path $work 'a.download.bin'
    Invoke-Download $port $a.fileId $downloadA
    Write-Host "== Checksums of A:"
    $checks += Compare-Hashes $fileA $downloadA $a.sha256

    # 4. File B while node 1 is dead: every chunk must get its 2 copies on nodes 0 and 2.
    Write-Host "== File B, uploaded while node 1 is dead"
    $fileB = Join-Path $work 'b.bin'
    [void](New-RandomFile $fileB 5)
    $b = Invoke-Upload $port $fileB
    $placementB = Get-ChunkPlacement $nodes $b.fileId $b.chunkCount
    Write-Host "== Where the 2 copies of each chunk of B are:"
    Write-Placement $placementB
    $onZeroAndTwo = $true
    foreach ($holders in $placementB) {
        if (($holders -join ',') -ne '0,2') { $onZeroAndTwo = $false }
    }
    Write-Host "   every chunk of B is on nodes 0 and 2: $onZeroAndTwo"
    $checks += $onZeroAndTwo

    # 5. B is readable too.
    $downloadB = Join-Path $work 'b.download.bin'
    Invoke-Download $port $b.fileId $downloadB
    Write-Host "== Checksums of B:"
    $checks += Compare-Hashes $fileB $downloadB $b.sha256

    # 6. What the server logged about node 1.
    Write-Host "== The server's log lines about the dead node:"
    $lines = @((Read-Shared $cluster.Server.ErrLog) -split "`r?`n" |
        Where-Object { $_ -match 'skipped unreachable|could not be read' })
    foreach ($line in $lines) { Write-Host "   $line" }
    $checks += ($lines.Count -gt 0)

    if (@($checks | Where-Object { -not $_ }).Count -eq 0) {
        Write-Host "== RESULT: SUCCESS (with node 1 dead, A was downloaded intact, and B got 2 copies of every chunk and was downloaded intact)"
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
