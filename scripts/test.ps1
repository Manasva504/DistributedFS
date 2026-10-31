<#
.SYNOPSIS
    Compiles DistributedFS with plain javac and runs its test suites.

.DESCRIPTION
    Used by the GitHub Actions workflow (.github/workflows/ci.yml) and usable locally from any
    folder. Every run compiles src/main/java and src/test/java into a brand-new folder in the
    system temp folder (distributedfs-build-<random>), so classes left over from deleted or renamed
    sources can never hide an error; the repository's own out/ and out-test/ folders are never
    touched. It then runs every *Test suite, or with -Stress the stress tests with a 16 MiB heap,
    and finally deletes the build folder it created. Each suite prints PASS, FAIL or SKIP per test;
    this script repeats the summary of every suite, lists every skipped test by name, and exits
    with code 1 if anything failed to compile or any suite failed. Skipped tests are reported as
    skipped, never as passed.

    Works in Windows PowerShell 5.1 and PowerShell 7.

.EXAMPLE
    .\scripts\test.ps1
    .\scripts\test.ps1 -Stress
    .\scripts\test.ps1 -Suite StorageNodeTest,FileServiceTest
#>
param(
    # Run the stress tests (with a 16 MiB heap) instead of the normal tests.
    [switch]$Stress,
    # Run only these suites, for example -Suite StorageNodeTest,FileServiceTest.
    [string[]]$Suite = @()
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
# A new folder with a random name, created by this run only, so deleting it at the end can never
# remove anything else.
$buildDir = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-build-" + [guid]::NewGuid().ToString('N'))
$mainOut = Join-Path $buildDir 'main'
$testOut = Join-Path $buildDir 'test'
Push-Location $root
try {
    New-Item -ItemType Directory -Path $buildDir | Out-Null
    $stressSuites = @('FileReconstructorTest', 'ChecksumServiceTest', 'StorageNodeTest', 'StorageClusterTest',
        'DistributedFsDemoTest', 'UploadEndpointTest', 'CrashRecoveryTest', 'DownloadEndpointTest', 'RemoteStorageNodeTest')

    Write-Host "== Build ($(& javac -version)) into $buildDir"
    foreach ($part in @(@{ Src = 'src\main\java'; Out = $mainOut; Cp = $null; List = 'main-sources.txt' },
                        @{ Src = 'src\test\java'; Out = $testOut; Cp = $mainOut; List = 'test-sources.txt' })) {
        # An argument file instead of a wildcard, so the build does not depend on how the shell or
        # the javac launcher expands *.java.
        $sources = Get-ChildItem -Path $part.Src -Recurse -Filter *.java | ForEach-Object { '"' + ($_.FullName -replace '\\', '/') + '"' }
        $listFile = Join-Path $buildDir $part.List
        [System.IO.File]::WriteAllLines($listFile, [string[]]$sources)
        $javacArgs = @('-encoding', 'UTF-8', '-Xlint:all', '-d', $part.Out)
        if ($part.Cp) { $javacArgs += @('-cp', $part.Cp) }
        & javac @javacArgs "@$listFile"
        $code = $LASTEXITCODE
        if ($code -ne 0) { Write-Host "Compilation of $($part.Src) failed (exit $code)"; exit 1 }
    }
    # The web console's files are read from the class path (javac copies nothing), so the
    # resources folder is on it, as for the server itself.
    $classPath = "$mainOut;$testOut;" + (Join-Path $root 'src\main\resources')

    # With powershell -File, "-Suite A,B" arrives as the single string "A,B", so split it here.
    $Suite = @($Suite | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    if ($Suite.Count -gt 0) {
        $suites = $Suite
    } elseif ($Stress) {
        $suites = $stressSuites
    } else {
        $suites = Get-ChildItem -Path 'src\test\java\com\distributedfs' -Filter '*Test.java' |
            Sort-Object Name | ForEach-Object { $_.BaseName }
    }

    $failedSuites = @()
    $skipped = @()
    $totals = @{ Passed = 0; Failed = 0; Skipped = 0 }
    $rows = @()
    foreach ($suite in $suites) {
        $javaArgs = @()
        if ($Stress) { $javaArgs += '-Xmx16m' }
        $javaArgs += @('-cp', $classPath, "com.distributedfs.$suite")
        if ($Stress) { $javaArgs += '--stress' }
        Write-Host ""
        Write-Host "== $suite$(if ($Stress) { ' (stress, -Xmx16m)' })"
        $watch = [Diagnostics.Stopwatch]::StartNew()
        $lines = @(& java @javaArgs)
        $code = $LASTEXITCODE
        $watch.Stop()
        $lines | ForEach-Object { Write-Host $_ }
        $summary = $lines | Where-Object { $_ -match '^\d+ passed, \d+ failed' } | Select-Object -Last 1
        if ($summary -match '^(\d+) passed, (\d+) failed(, (\d+) skipped)?') {
            $totals.Passed += [int]$Matches[1]
            $totals.Failed += [int]$Matches[2]
            if ($Matches[4]) { $totals.Skipped += [int]$Matches[4] }
        }
        $lines | Where-Object { $_ -like 'SKIP  *' } | ForEach-Object { $skipped += "$suite`: $($_.Substring(6))" }
        if ($code -ne 0 -or -not $summary) { $failedSuites += $suite }
        $rows += "| $suite | $(if ($code -eq 0 -and $summary) { 'pass' } else { 'FAIL' }) | $summary | $([math]::Round($watch.Elapsed.TotalSeconds, 1)) s |"
    }

    Write-Host ""
    Write-Host "== Result: $($suites.Count) suites, $($totals.Passed) passed, $($totals.Failed) failed, $($totals.Skipped) skipped"
    foreach ($s in $skipped) { Write-Host "   skipped: $s" }
    if ($failedSuites.Count -gt 0) { Write-Host "   FAILED suites: $($failedSuites -join ', ')" }

    if ($env:GITHUB_STEP_SUMMARY) {
        $md = @("### DistributedFS $(if ($Stress) { 'stress' } else { 'normal' }) tests", '',
            "$($suites.Count) suites: **$($totals.Passed) passed, $($totals.Failed) failed, $($totals.Skipped) skipped**", '',
            '| Suite | Status | Summary | Time |', '|---|---|---|---|') + $rows
        if ($skipped.Count -gt 0) { $md += @('', 'Skipped tests (not run, not passed):', '') + ($skipped | ForEach-Object { "- $_" }) }
        $md | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Append -Encoding utf8
    }

    if ($failedSuites.Count -gt 0) { exit 1 }
    exit 0
} finally {
    Pop-Location
    # Only the folder this run created; it holds nothing but compiled classes and source lists.
    if (Test-Path -LiteralPath $buildDir) {
        Remove-Item -LiteralPath $buildDir -Recurse -Force -ErrorAction Continue
    }
}
