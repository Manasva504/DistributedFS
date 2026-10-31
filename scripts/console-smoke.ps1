<#
.SYNOPSIS
    Smoke test of the web console in a real browser: starts a cluster, loads http://127.0.0.1:<port>/ in headless Chrome or Edge, checks the page, and saves screenshots.

.DESCRIPTION
    Run by hand; it is not part of scripts\test.ps1 or CI (it needs a browser, and headless timing
    depends on the machine). It uses the Chrome or Edge already installed, found at the standard
    paths; nothing is downloaded. It:

      1. builds src/main/java into a new temporary folder and starts three storage node processes
         and the server (as scripts\demo.ps1 does), then uploads two random files;
      2. checks the response headers of GET /, /console/console.css and /console/console.js
         (Content-Security-Policy, nosniff, X-Frame-Options, Cache-Control, content types), and that
         a POST /cluster/repair with a foreign Origin is refused;
      3. healthy cluster: loads the page with --dump-dom and checks that the scripts ran and polled
         (data-console-ready), that no JavaScript error or Content-Security-Policy violation was
         reported (the page counts both on <body>), and that the page shows the three node IDs, both
         files, chunk tiles and "Healthy"; then saves screenshots at 1440x900, 390x844 and a tall
         390x2400 one that shows the stacked panels, each at exactly that viewport (through the
         browser's DevTools protocol on a loopback port), and checks that the page does not
         scroll sideways at that width;
      4. through the page itself (its file input, its upload, its download link), uploads report.pdf
         and a name with accents and a space, and checks that the server stored each under its
         name, that the download names it in Content-Disposition, and that Chrome saves it under
         exactly the same name with the same SHA-256;
      5. kills node 1, waits until GET /cluster/status shows it DOWN, and repeats step 3 for the
         degraded cluster ("Degraded", node 1 DOWN, unreachable copies), with screenshots;
      6. stops everything it started and deletes its work folder. The screenshots stay in -OutDir.

    Exit code 0 only if every check passed. Works in Windows PowerShell 5.1 and PowerShell 7.

.EXAMPLE
    .\scripts\console-smoke.ps1
    .\scripts\console-smoke.ps1 -OutDir C:\temp\console-shots -Browser 'C:\Program Files\Google\Chrome\Application\chrome.exe'
#>
param(
    # Where the screenshots go; a new folder in the system temp folder by default.
    [string]$OutDir,
    # The browser to use; the first of Chrome and Edge found at their standard paths by default.
    [string]$Browser,
    # Keep the work folder (node folders, server data, logs, browser output).
    [switch]$Keep
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'cluster-common.ps1')
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-console-smoke-" + [guid]::NewGuid().ToString('N'))
$classes = Join-Path $work 'classes'
$started = @()
$java = $null
$exitCode = 1
if (-not $OutDir) {
    $OutDir = Join-Path ([System.IO.Path]::GetTempPath()) ("distributedfs-console-shots-" + (Get-Date -Format 'yyyyMMdd-HHmmss'))
}
$checks = New-Object System.Collections.Generic.List[object]

function Add-Check([string]$what, [bool]$ok) {
    $checks.Add([pscustomobject]@{ What = $what; Ok = $ok })
    Write-Host ("   [{0}] {1}" -f $(if ($ok) { 'ok' } else { 'FAIL' }), $what)
}

function Find-Browser {
    $candidates = @(
        (Join-Path $env:ProgramFiles 'Google\Chrome\Application\chrome.exe'),
        (Join-Path ${env:ProgramFiles(x86)} 'Google\Chrome\Application\chrome.exe'),
        (Join-Path $env:LOCALAPPDATA 'Google\Chrome\Application\chrome.exe'),
        (Join-Path ${env:ProgramFiles(x86)} 'Microsoft\Edge\Application\msedge.exe'),
        (Join-Path $env:ProgramFiles 'Microsoft\Edge\Application\msedge.exe'))
    foreach ($path in $candidates) {
        if ($path -and (Test-Path -LiteralPath $path)) { return $path }
    }
    throw "no Chrome or Edge found at the standard paths; pass -Browser <path to chrome.exe or msedge.exe>"
}

# Runs the headless browser once with a fresh profile; returns its standard output. Kills it after 90 s.
$browserRun = 0
function Invoke-Browser([string[]]$browserArgs) {
    $script:browserRun++
    $profileDir = Join-Path $work "browser-profile-$script:browserRun"
    $outLog = Join-Path $work "browser-$script:browserRun.out.log"
    $errLog = Join-Path $work "browser-$script:browserRun.err.log"
    $all = @('--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--disable-extensions',
        '--disable-sync', '--disable-background-networking', '--hide-scrollbars', "--user-data-dir=$profileDir") + $browserArgs
    $quoted = $all | ForEach-Object { '"' + $_ + '"' }
    $process = Start-Process -FilePath $Browser -ArgumentList $quoted -NoNewWindow -PassThru `
        -RedirectStandardOutput $outLog -RedirectStandardError $errLog
    if (-not $process.WaitForExit(90000)) {
        Stop-StartedProcess $process
        throw "the browser did not finish within 90 s"
    }
    return (Read-Shared $outLog)
}

# Screenshots go through the browser's DevTools protocol on a loopback port, not --screenshot:
# headless Chrome never makes its window narrower than about 500 px, so --window-size=390 would lay
# the page out wider than a phone and crop it. Emulation.setDeviceMetricsOverride sets an exact
# viewport (as a phone, for widths under 600). The browser runs with its own temporary profile and
# is stopped afterwards; the DevTools port listens on 127.0.0.1 only.
$devtools = $null

function Start-DevToolsBrowser {
    $profileDir = Join-Path $work 'browser-profile-devtools'
    $all = @('--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--disable-extensions',
        '--disable-sync', '--disable-background-networking', '--hide-scrollbars', '--remote-debugging-address=127.0.0.1',
        '--remote-debugging-port=0', "--user-data-dir=$profileDir", 'about:blank')
    $quoted = $all | ForEach-Object { '"' + $_ + '"' }
    $process = Start-Process -FilePath $Browser -ArgumentList $quoted -NoNewWindow -PassThru `
        -RedirectStandardOutput (Join-Path $work 'browser-devtools.out.log') -RedirectStandardError (Join-Path $work 'browser-devtools.err.log')
    $script:started += $process
    $portFile = Join-Path $profileDir 'DevToolsActivePort'
    $deadline = (Get-Date).AddSeconds(30)
    while (-not (Test-Path -LiteralPath $portFile) -or (Read-Shared $portFile).Trim() -eq '') {
        if ($process.HasExited -or (Get-Date) -gt $deadline) { throw "the browser's DevTools port did not appear" }
        Start-Sleep -Milliseconds 100
    }
    $devPort = [int]((Read-Shared $portFile) -split "`r?`n")[0]
    $pages = (New-Client).DownloadString("http://127.0.0.1:$devPort/json/list") | ConvertFrom-Json
    $page = @($pages | Where-Object { $_.type -eq 'page' })[0]
    if (-not $page) { throw "the browser has no page to drive" }
    $socket = New-Object System.Net.WebSockets.ClientWebSocket
    $socket.ConnectAsync([Uri]$page.webSocketDebuggerUrl, [Threading.CancellationToken]::None).Wait()
    $script:devtools = @{ Socket = $socket; Id = 0 }
}

# Sends one DevTools command and returns the raw JSON text of its answer (events in between are skipped).
function Invoke-DevTools([string]$method, [string]$paramsJson) {
    $socket = $script:devtools.Socket
    $script:devtools.Id++
    $id = $script:devtools.Id
    $message = '{"id":' + $id + ',"method":"' + $method + '","params":' + $paramsJson + '}'
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($message)
    $socket.SendAsync((New-Object System.ArraySegment[byte] -ArgumentList @(, $bytes)),
        [System.Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).Wait()
    $buffer = New-Object byte[] 65536
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        $stream = New-Object System.IO.MemoryStream
        do {
            $task = $socket.ReceiveAsync((New-Object System.ArraySegment[byte] -ArgumentList @(, $buffer)),
                [Threading.CancellationToken]::None)
            if (-not $task.Wait(60000)) { throw "no answer to $method" }
            $stream.Write($buffer, 0, $task.Result.Count)
        } while (-not $task.Result.EndOfMessage)
        $text = [System.Text.Encoding]::UTF8.GetString($stream.ToArray())
        if ($text.StartsWith('{"id":' + $id + ',')) {
            if ($text.Contains('"error":{')) { throw "$method failed: $text" }
            return $text
        }
    }
    throw "no answer to $method within 60 s"
}

# Shows the page at exactly $width x $height CSS pixels, lets it poll for $waitSeconds, checks that it
# has no horizontal scroll, and saves a PNG screenshot.
function Save-Screenshot([string]$url, [int]$width, [int]$height, [string]$name) {
    if (-not $script:devtools) { Start-DevToolsBrowser }
    $mobile = if ($width -lt 600) { 'true' } else { 'false' }
    [void](Invoke-DevTools 'Emulation.setDeviceMetricsOverride' ('{"width":' + $width + ',"height":' + $height +
        ',"deviceScaleFactor":1,"mobile":' + $mobile + '}'))
    [void](Invoke-DevTools 'Page.navigate' ('{"url":"' + $url + '"}'))
    $ready = $false
    $deadline = (Get-Date).AddSeconds(20)
    while (-not $ready -and (Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 500
        $answer = Invoke-DevTools 'Runtime.evaluate' '{"expression":"document.body !== null && document.body.getAttribute(\"data-console-ready\") === \"true\"","returnByValue":true}'
        $ready = $answer.Contains('"value":true')
    }
    Start-Sleep -Seconds 3 # a few more polls, so the "last good check" times are live
    $layout = Invoke-DevTools 'Runtime.evaluate' '{"expression":"[window.innerWidth, document.documentElement.scrollWidth].join(\"x\")","returnByValue":true}'
    $sizes = [regex]::Match($layout, '"value":"(\d+)x(\d+)"')
    $viewport = [int]$sizes.Groups[1].Value
    $scrollWidth = [int]$sizes.Groups[2].Value
    $fits = $ready -and $viewport -eq $width -and $scrollWidth -le $viewport
    $culprits = ''
    if (-not $fits) {
        # Name the elements that stick out (a scroll box's own content may, so it is left out).
        $expression = 'Array.from(document.querySelectorAll(\"body *\")).filter(function (e) { return !e.closest(\".backdrop\")' +
            ' && !e.closest(\".map-scroll\") && e.getBoundingClientRect().right > ' + $width + ' + 0.5; }).slice(0, 8).map(function (e) { return' +
            ' e.tagName.toLowerCase() + (e.className && e.className.baseVal === undefined ? \".\" + e.className.split(\" \").join(\".\") : \"\")' +
            ' + \" (right \" + Math.round(e.getBoundingClientRect().right) + \")\"; }).join(\", \")'
        $answer = Invoke-DevTools 'Runtime.evaluate' ('{"expression":"' + $expression + '","returnByValue":true}')
        $culprits = ' -- wider than the viewport: ' + [regex]::Match($answer, '"value":"([^"]*)"').Groups[1].Value
    }
    Add-Check ("at $width x $height the page is $viewport px wide and does not scroll sideways (content $scrollWidth px)" + $culprits) $fits
    $shot = Invoke-DevTools 'Page.captureScreenshot' '{"format":"png"}'
    $data = [regex]::Match($shot, '"data":"([A-Za-z0-9+/=]+)"').Groups[1].Value
    $path = Join-Path $OutDir $name
    [System.IO.File]::WriteAllBytes($path, [Convert]::FromBase64String($data))
    Add-Check "screenshot $name ($width x $height) saved" ((Get-Item -LiteralPath $path).Length -gt 10000)
    Test-Contrast $path $name
}

# A string as a JSON string literal: printable ASCII stays, everything else is a backslash-u escape.
function ConvertTo-JsonText([string]$s) {
    $backslash = [char]92
    $out = New-Object System.Text.StringBuilder
    [void]$out.Append('"')
    foreach ($ch in $s.ToCharArray()) {
        $c = [int]$ch
        if ($c -eq 34 -or $c -eq 92) {
            [void]$out.Append($backslash).Append($ch)
        } elseif ($c -lt 0x20 -or $c -gt 0x7E) {
            [void]$out.Append($backslash).Append('u').Append($c.ToString('x4'))
        } else {
            [void]$out.Append($ch)
        }
    }
    [void]$out.Append('"')
    return $out.ToString()
}

# The RFC 8187 form of a name, as filename*=UTF-8''<this> carries it.
function Get-FilenameStar([string]$name) {
    $out = New-Object System.Text.StringBuilder
    foreach ($b in [System.Text.Encoding]::UTF8.GetBytes($name)) {
        $ch = [char]$b
        if ($b -lt 0x80 -and ([char]::IsLetterOrDigit($ch) -or '!#$&+-.^_`|~'.Contains([string]$ch))) {
            [void]$out.Append($ch)
        } else {
            [void]$out.Append('%').Append($b.ToString('X2'))
        }
    }
    return $out.ToString()
}

# The requirement that a user never renames a file: through the page itself (its file input, its
# upload code, its download link), report.pdf and "resume 2026.pdf" with accents must come back
# from Chrome's download under exactly the same name, with the same bytes.
function Test-NamedUploads([string]$url, [int]$port) {
    if (-not $script:devtools) { Start-DevToolsBrowser }
    $names = @('report.pdf', ('r' + [char]0xE9 + 'sum' + [char]0xE9 + ' 2026.pdf'))
    $source = Join-Path $work 'named-uploads'
    $downloads = Join-Path $work 'downloads'
    New-Item -ItemType Directory -Path $source, $downloads | Out-Null
    $allow = '{"behavior":"allow","downloadPath":' + (ConvertTo-JsonText $downloads) + '}'
    try {
        [void](Invoke-DevTools 'Browser.setDownloadBehavior' $allow)
    } catch {
        [void](Invoke-DevTools 'Page.setDownloadBehavior' $allow)
    }
    [void](Invoke-DevTools 'Emulation.setDeviceMetricsOverride' '{"width":1440,"height":900,"deviceScaleFactor":1,"mobile":false}')
    [void](Invoke-DevTools 'Page.navigate' ('{"url":' + (ConvertTo-JsonText $url) + '}'))
    Start-Sleep -Seconds 2
    foreach ($name in $names) {
        $path = Join-Path $source $name
        [void](New-RandomFile $path 1)
        $expectedHash = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash
        # Hand the file to the page's own file input, as a person choosing it would.
        $inputElement = Invoke-DevTools 'Runtime.evaluate' '{"expression":"document.getElementById(\"file-input\")"}'
        $objectId = [regex]::Match($inputElement, '"objectId":"([^"]+)"').Groups[1].Value
        [void](Invoke-DevTools 'DOM.setFileInputFiles' ('{"files":[' + (ConvertTo-JsonText $path) + '],"objectId":' +
            (ConvertTo-JsonText $objectId) + '}'))
        $file = $null
        $deadline = (Get-Date).AddSeconds(30)
        while (-not $file -and (Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 300
            $list = (New-Client).DownloadString("http://127.0.0.1:$port/files") | ConvertFrom-Json
            $file = @($list.files | Where-Object { $_.name -eq $name })[0]
        }
        Add-Check "the page uploaded `"$name`" and the server stored it under that name" ($null -ne $file)
        if (-not $file) { continue }
        $answer = Get-Headers "http://127.0.0.1:$port/files/$($file.fileId)"
        $disposition = $answer.Headers['Content-Disposition']
        Add-Check "GET /files/<id> names it in Content-Disposition: $disposition" (
            $disposition.EndsWith("; filename*=UTF-8''" + (Get-FilenameStar $name)) -and $disposition.StartsWith('attachment; filename="'))
        # Click the page's own download link for it, once the page lists the file.
        $click = '(function () { var name = ' + (ConvertTo-JsonText $name) + '; var links = Array.prototype.filter.call(' +
            'document.querySelectorAll("a[download]"), function (a) { return a.getAttribute("download") === name; });' +
            ' if (links.length !== 1) { return false; } links[0].click(); return true; })()'
        $clicked = $false
        $deadline = (Get-Date).AddSeconds(20)
        while (-not $clicked -and (Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 500
            $result = Invoke-DevTools 'Runtime.evaluate' ('{"expression":' + (ConvertTo-JsonText $click) + ',"returnByValue":true}')
            $clicked = $result.Contains('"value":true')
        }
        Add-Check "the page shows a download link named `"$name`"" $clicked
        $saved = Join-Path $downloads $name
        $deadline = (Get-Date).AddSeconds(30)
        $ok = $false
        while (-not $ok -and (Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 300
            $ok = (Test-Path -LiteralPath $saved) -and ((Get-Item -LiteralPath $saved).Length -eq (Get-Item -LiteralPath $path).Length)
        }
        $sameBytes = $ok -and ((Get-FileHash -LiteralPath $saved -Algorithm SHA256).Hash -eq $expectedHash)
        Add-Check "Chrome saved it as `"$name`", byte for byte" $sameBytes
    }
    # While a download runs, Chrome keeps it under a temporary name; at the end only the two remain.
    $expected = ($names | Sort-Object) -join ' | '
    $deadline = (Get-Date).AddSeconds(15)
    do {
        $got = (@(Get-ChildItem -LiteralPath $downloads -File | ForEach-Object { $_.Name }) | Sort-Object) -join ' | '
        if ($got -ne $expected) { Start-Sleep -Milliseconds 300 }
    } while ($got -ne $expected -and (Get-Date) -lt $deadline)
    Add-Check "the download folder holds exactly: $got" ($got -eq $expected)
}

# WCAG relative luminance of an sRGB color.
function Get-Luminance([double]$r, [double]$g, [double]$b) {
    $channels = foreach ($c in @($r, $g, $b)) {
        $v = $c / 255
        if ($v -le 0.03928) { $v / 12.92 } else { [Math]::Pow(($v + 0.055) / 1.055, 2.4) }
    }
    return 0.2126 * $channels[0] + 0.7152 * $channels[1] + 0.0722 * $channels[2]
}

# Measures the contrast of every visible run of text on the page as it was just captured: the
# text's color comes from the page, its background from the screenshot's pixels around the text
# (those whose brightness is farthest from the text's), since a frosted panel's background is
# whatever is behind it. WCAG AA: 4.5:1, or 3:1 for large text (24 px, or 18.66 px bold). The
# middle dot of an empty chunk tile is decoration (the tile also says "no copy" to screen readers).
function Test-Contrast([string]$path, [string]$name) {
    $expression = 'JSON.stringify(Array.from(document.querySelectorAll(\"body *\")).filter(function (e) {' +
        ' return !e.closest(\".visually-hidden\") && !e.closest(\"[hidden]\") && !e.closest(\"svg\"); }).map(function (e) {' +
        ' var node = Array.from(e.childNodes).filter(function (n) { return n.nodeType === 3 && n.textContent.trim() !== \"\"; })[0];' +
        ' if (!node) { return null; } var range = document.createRange(); range.selectNodeContents(node);' +
        ' var r = range.getBoundingClientRect(); var s = getComputedStyle(e);' +
        ' return { t: node.textContent.trim().slice(0, 30), c: s.color, f: parseFloat(s.fontSize), w: parseInt(s.fontWeight, 10),' +
        ' x: Math.floor(r.left), y: Math.floor(r.top), wd: Math.ceil(r.width), ht: Math.ceil(r.height) }; }).filter(function (i) {' +
        ' return i && i.t !== \"\u00b7\" && i.wd >= 4 && i.ht >= 4 && i.x >= 0 && i.y >= 0 && i.x + i.wd <= innerWidth && i.y + i.ht <= innerHeight; }))'
    $answer = Invoke-DevTools 'Runtime.evaluate' ('{"expression":"' + $expression + '","returnByValue":true}')
    $items = ($answer | ConvertFrom-Json).result.result.value | ConvertFrom-Json
    Add-Type -AssemblyName System.Drawing
    $bitmap = New-Object System.Drawing.Bitmap $path
    $failures = @()
    $worst = $null
    try {
        foreach ($item in $items) {
            $rgb = [regex]::Matches($item.c, '[\d.]+') | ForEach-Object { [double]$_.Value }
            $textLum = Get-Luminance $rgb[0] $rgb[1] $rgb[2]
            $samples = @()
            $step = [Math]::Max(1, [int][Math]::Floor([Math]::Min($item.wd, $item.ht) / 6))
            for ($y = $item.y; $y -lt [Math]::Min($bitmap.Height, $item.y + $item.ht); $y += $step) {
                for ($x = $item.x; $x -lt [Math]::Min($bitmap.Width, $item.x + $item.wd); $x += $step) {
                    $p = $bitmap.GetPixel($x, $y)
                    $samples += , @((Get-Luminance $p.R $p.G $p.B))
                }
            }
            if ($samples.Count -eq 0) { continue }
            $farthest = @($samples | Sort-Object { [Math]::Abs($_[0] - $textLum) } -Descending |
                Select-Object -First ([Math]::Max(1, [int][Math]::Ceiling($samples.Count / 5))))
            $bgLum = ($farthest | ForEach-Object { $_[0] } | Measure-Object -Average).Average
            $ratio = ([Math]::Max($textLum, $bgLum) + 0.05) / ([Math]::Min($textLum, $bgLum) + 0.05)
            $large = $item.f -ge 24 -or ($item.f -ge 18.66 -and $item.w -ge 700)
            $needed = if ($large) { 3.0 } else { 4.5 }
            $margin = $ratio / $needed
            if ($null -eq $worst -or $margin -lt $worst.Margin) {
                $worst = @{ Margin = $margin; Ratio = $ratio; Needed = $needed; Text = $item.t }
            }
            if ($ratio -lt $needed) {
                $failures += ('"{0}" {1:N2}:1 (needs {2}:1)' -f $item.t, $ratio, $needed)
            }
        }
    } finally {
        $bitmap.Dispose()
    }
    $detail = if ($worst) { ' -- lowest: "{0}" {1:N2}:1, needs {2}:1' -f $worst.Text, $worst.Ratio, $worst.Needed } else { '' }
    if ($failures.Count -gt 0) { $detail += ' -- below: ' + (($failures | Select-Object -First 6) -join '; ') }
    Add-Check ("text contrast in $name passes WCAG AA ($(@($items).Count) text runs measured)" + $detail) (
        @($items).Count -gt 10 -and $failures.Count -eq 0)
}

# Loads the page, lets it poll, and returns the DOM after its scripts ran.
function Get-RenderedDom([string]$url) {
    return (Invoke-Browser @('--window-size=1440,900', '--virtual-time-budget=8000', '--dump-dom', $url))
}

function Test-Dom([string]$dom, $nodes, [string[]]$fileIds, [string]$health, [int]$downNode) {
    Add-Check "the scripts ran and polled the API (data-console-ready)" ($dom -match 'data-console-ready="true"')
    Add-Check "no JavaScript error was reported" (-not ($dom -match 'data-js-errors="'))
    Add-Check "no Content-Security-Policy violation was reported" (-not ($dom -match 'data-csp-violations="'))
    Add-Check "the health line says $health" ($dom -match "<strong id=`"health-title`">$health</strong>")
    for ($i = 0; $i -lt $nodes.Count; $i++) {
        Add-Check "node $i's ID is shown" ($dom.Contains($nodes[$i].Id))
    }
    foreach ($id in $fileIds) {
        Add-Check "file $id is listed" ($dom.Contains("title=`"$id`""))
    }
    $copies = ([regex]::Matches($dom, 'class="tile tile-(copy|new|extra)"')).Count
    Add-Check "the chunk map shows copies ($copies tiles)" ($copies -gt 0)
    if ($downNode -ge 0) {
        Add-Check "node $downNode is shown DOWN" ($dom -match "aria-label=`"Node $downNode, DOWN`"")
        $unreachable = ([regex]::Matches($dom, 'class="tile tile-unreachable"')).Count
        Add-Check "the chunk map shows node $downNode's copies as unreachable ($unreachable tiles)" ($unreachable -gt 0)
    } else {
        Add-Check "every node is shown UP" (-not ($dom -match ', DOWN"'))
    }
}

function Get-Headers([string]$url) {
    $request = [System.Net.HttpWebRequest]::Create($url)
    $request.Proxy = $null
    $response = $request.GetResponse()
    try {
        return @{ Status = [int]$response.StatusCode; Headers = $response.Headers }
    } finally {
        $response.Close()
    }
}

function Wait-NodeDown([int]$port, [int]$index) {
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $status = (New-Client).DownloadString("http://127.0.0.1:$port/cluster/status") | ConvertFrom-Json
        if ($status.nodes[$index].state -eq 'DOWN') { return $true }
        Start-Sleep -Milliseconds 250
    }
    return $false
}

try {
    New-Item -ItemType Directory -Path $work | Out-Null
    New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
    Write-Host "== Work folder: $work"
    Write-Host "== Screenshots: $OutDir"
    if (-not $Browser) { $Browser = Find-Browser }
    $version = (Get-Item -LiteralPath $Browser).VersionInfo.ProductVersion
    Write-Host "== Browser: $Browser ($version)"
    Build-Classes $root
    $cluster = Start-Cluster 3
    $nodes = $cluster.Nodes
    $port = $cluster.Port
    $url = "http://127.0.0.1:$port/"

    $fileA = Join-Path $work 'a.bin'
    [void](New-RandomFile $fileA 5)
    $a = Invoke-Upload $port $fileA
    $fileB = Join-Path $work 'b.bin'
    [void](New-RandomFile $fileB 3)
    $b = Invoke-Upload $port $fileB

    Write-Host "== Headers"
    $expectedCsp = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self';" +
        " base-uri 'none'; form-action 'none'; frame-ancestors 'none'; require-trusted-types-for 'script'"
    foreach ($asset in @(@{ Path = '/'; Type = 'text/html; charset=utf-8' },
                         @{ Path = '/console/console.css'; Type = 'text/css; charset=utf-8' },
                         @{ Path = '/console/console.js'; Type = 'text/javascript; charset=utf-8' })) {
        $answer = Get-Headers "http://127.0.0.1:$port$($asset.Path)"
        $h = $answer.Headers
        Add-Check ("GET {0}: 200, {1}, CSP, nosniff, DENY, no-store" -f $asset.Path, $asset.Type) (
            $answer.Status -eq 200 -and $h['Content-Type'] -eq $asset.Type -and $h['Content-Security-Policy'] -eq $expectedCsp -and
            $h['X-Content-Type-Options'] -eq 'nosniff' -and $h['X-Frame-Options'] -eq 'DENY' -and $h['Cache-Control'] -eq 'no-store')
    }
    $evil = [System.Net.HttpWebRequest]::Create("http://127.0.0.1:$port/cluster/repair")
    $evil.Method = 'POST'
    $evil.Proxy = $null
    $evil.ContentLength = 0
    $evil.Headers.Add('Origin', 'http://evil.example')
    $refused = $false
    try {
        $evil.GetResponse().Close()
    } catch [System.Net.WebException] {
        $refused = $null -ne $_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 403
        if ($_.Exception.Response) { $_.Exception.Response.Close() }
    }
    Add-Check "POST /cluster/repair with Origin: http://evil.example is refused with 403" $refused

    Write-Host "== Healthy cluster"
    Test-Dom (Get-RenderedDom $url) $nodes @($a.fileId, $b.fileId) 'Healthy' -1
    Save-Screenshot $url 1440 900 'console-healthy-1440x900.png'
    Save-Screenshot $url 390 844 'console-healthy-390x844.png'
    Save-Screenshot $url 390 2400 'console-healthy-390x2400-tall.png'

    Write-Host "== Named uploads and downloads through the page"
    Test-NamedUploads $url $port

    Write-Host "== Degraded cluster: killing node 1 (pid $($nodes[1].Process.Id))"
    Stop-StartedProcess $nodes[1].Process
    Add-Check "node 1 is DOWN in GET /cluster/status" (Wait-NodeDown $port 1)
    Test-Dom (Get-RenderedDom $url) $nodes @($a.fileId, $b.fileId) 'Degraded' 1
    Save-Screenshot $url 1440 900 'console-degraded-1440x900.png'
    Save-Screenshot $url 390 844 'console-degraded-390x844.png'

    $failed = @($checks | Where-Object { -not $_.Ok })
    if ($failed.Count -eq 0) {
        Write-Host "== RESULT: SUCCESS ($($checks.Count) checks passed; screenshots in $OutDir)"
        $exitCode = 0
    } else {
        Write-Host "== RESULT: FAILURE ($($failed.Count) of $($checks.Count) checks failed)"
    }
} catch {
    Write-Host "== RESULT: FAILURE ($($_.Exception.Message))"
} finally {
    if (-not (Stop-AndCleanUp $Keep.IsPresent)) { $exitCode = 1 }
}
exit $exitCode
