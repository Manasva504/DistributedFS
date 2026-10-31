<#
.SYNOPSIS
    Starts three storage nodes and the server, uploads two files, and opens the web console in your browser; keys kill and restart node 1.

.DESCRIPTION
    Builds src/main/java into a new temporary folder and starts three storage node processes and
    the DistributedFS server on free ports (as scripts\demo.ps1 does), uploads two random files
    (5 MiB and 3 MiB; -Empty skips this), prints the console's address and opens it in the default
    browser (-NoBrowser only prints it). Then it waits for a key:

      k   kill node 1's process (the console shows it DOWN within about 6 seconds)
      r   restart node 1 on its old folder and port (UP again within about 2 seconds)
      q   stop everything and delete the work folder (also Ctrl+C)

    Use the console's Dry run and Repair buttons to see the repair copy chunks while node 1 is
    down, and trim the extra copies after it is back. If the script's input is not a console (for
    example a pipe), it reads the same letters one per line.

    Everything lives in one new folder in the system temp folder (distributedfs-console-demo-<random>),
    deleted at the end unless -Keep is given. The process handling is shared with the other demos
    (scripts\cluster-common.ps1). Works in Windows PowerShell 5.1 and PowerShell 7.

.EXAMPLE
    .\scripts\console-demo.ps1
    .\scripts\console-demo.ps1 -Empty -NoBrowser
#>
param(
    # Do not upload the two example files.
    [switch]$Empty,
    # Print the console's address without opening a browser.
    [switch]$NoBrowser,
    # Keep the work folder (node folders, server data, logs).
    [switch]$Keep
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'cluster-common.ps1')
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-console-demo-" + [guid]::NewGuid().ToString('N'))
$classes = Join-Path $work 'classes'
$started = @()
$java = $null
$exitCode = 1

# The next letter typed: from the keyboard, or one per line when the input is redirected.
function Read-Command {
    if ([Console]::IsInputRedirected) {
        $line = [Console]::In.ReadLine()
        if ($null -eq $line) { return 'q' }
        return $line.Trim().ToLowerInvariant()
    }
    return ([string][Console]::ReadKey($true).KeyChar).ToLowerInvariant()
}

function Restart-Node($node) {
    for ($attempt = 1; $attempt -le 5; $attempt++) {
        try {
            $restarted = Start-JavaProcess "node-1-restart-$attempt-$(Get-Random)" @('com.distributedfs.StorageNodeServer',
                '--data-dir', $node.Folder, '--port', "$($node.Port)") 'DistributedFS storage node ([0-9a-f]{32}) listening on'
            $node.Process = $restarted.Process
            return $true
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    return $false
}

try {
    New-Item -ItemType Directory -Path $work | Out-Null
    Write-Host "== Work folder: $work"
    Build-Classes $root
    $cluster = Start-Cluster 3
    $nodes = $cluster.Nodes
    $url = "http://127.0.0.1:$($cluster.Port)/"
    if (-not $Empty) {
        foreach ($size in 5, 3) {
            $file = Join-Path $work "example-$size-mib.bin"
            [void](New-RandomFile $file $size)
            [void](Invoke-Upload $cluster.Port $file)
        }
    }
    Write-Host ""
    Write-Host "== Web console: $url"
    if (-not $NoBrowser) {
        Start-Process $url
    }
    Write-Host "== Keys: k = kill node 1, r = restart node 1, q = quit"
    while ($true) {
        $command = Read-Command
        if ($command -eq 'q') {
            break
        } elseif ($command -eq 'k') {
            if ($nodes[1].Process.HasExited) {
                Write-Host "   node 1 is not running"
            } else {
                Stop-StartedProcess $nodes[1].Process
                Write-Host "   killed node 1 (it is DOWN after 3 missed heartbeats, about 6 seconds)"
            }
        } elseif ($command -eq 'r') {
            if (-not $nodes[1].Process.HasExited) {
                Write-Host "   node 1 is already running"
            } elseif (Restart-Node $nodes[1]) {
                Write-Host "   restarted node 1 on port $($nodes[1].Port) (pid $($nodes[1].Process.Id))"
            } else {
                Write-Host "   node 1 could not be restarted on port $($nodes[1].Port)"
            }
        } elseif ($command -ne '') {
            Write-Host "   keys: k = kill node 1, r = restart node 1, q = quit"
        }
    }
    $exitCode = 0
} catch {
    Write-Host "== FAILURE ($($_.Exception.Message))"
} finally {
    if (-not (Stop-AndCleanUp $Keep.IsPresent)) { $exitCode = 1 }
}
exit $exitCode
