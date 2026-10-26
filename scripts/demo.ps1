<#
.SYNOPSIS
    Runs DistributedFS with three storage node processes: upload, download and verify a file.

.DESCRIPTION
    Builds src/main/java into a new temporary folder, starts three storage node processes
    (StorageNodeServer) and the DistributedFS server with one --node option per node, each on a
    free port that the script reads from the process's own ready line. It then uploads a random
    5 MiB file, downloads it again, compares the SHA-256 of both copies with Get-FileHash, and shows
    how many chunk copies each node holds (the server keeps 2 copies of every chunk).

    Everything lives in one new folder in the system temp folder (distributedfs-cluster-demo-<random>).
    At the end the script stops only the processes it started and deletes that folder, unless -Keep
    is given (the processes are stopped either way). The repository's out/ and out-test/ folders are
    never touched. Exit code 0 means the downloaded file is identical to the uploaded one.
    The process handling is shared with scripts\kill-demo.ps1 (scripts\cluster-common.ps1).

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
. (Join-Path $PSScriptRoot 'cluster-common.ps1')
# A new folder with a random name, created by this run only, so deleting it can never remove anything else.
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-cluster-demo-" + [guid]::NewGuid().ToString('N'))
$classes = Join-Path $work 'classes'
$started = @()
$java = $null
$exitCode = 1

try {
    New-Item -ItemType Directory -Path $work | Out-Null
    Write-Host "== Work folder: $work"
    Build-Classes $root
    $cluster = Start-Cluster 3

    $uploadFile = Join-Path $work 'upload.bin'
    $downloadFile = Join-Path $work 'download.bin'
    [void](New-RandomFile $uploadFile 5)
    $manifest = Invoke-Upload $cluster.Port $uploadFile
    Invoke-Download $cluster.Port $manifest.fileId $downloadFile
    Write-Host "== Checksums:"
    $same = Compare-Hashes $uploadFile $downloadFile $manifest.sha256
    Write-Host "== Chunk copies per node ($($manifest.chunkCount) chunks of $($manifest.chunkSize) bytes, 2 copies each):"
    Write-CopiesPerNode $cluster.Nodes
    if ($same) {
        Write-Host "== RESULT: SUCCESS (the downloaded file is identical to the uploaded one)"
        $exitCode = 0
    } else {
        Write-Host "== RESULT: FAILURE (the checksums differ)"
    }
} catch {
    Write-Host "== RESULT: FAILURE ($($_.Exception.Message))"
} finally {
    if (-not (Stop-AndCleanUp $Keep.IsPresent)) { $exitCode = 1 }
}
exit $exitCode
