param(
  [switch]$Debug
)

$ErrorActionPreference = "Stop"

function Bash-SingleQuote {
  param([string]$Text)
  # Wrap in bash single-quotes, escaping embedded single quotes safely.
  # In bash:  abc'def  ->  'abc'"'"'def'
  $rep = "'" + '"' + "'" + '"' + "'"
  return "'" + ($Text -replace "'", $rep) + "'"
}

function Write-Header($title) {
  Write-Host ""
  Write-Host ("=" * 78)
  Write-Host $title
  Write-Host ("=" * 78)
}

function Resolve-RepoRoot {
  # This script lives in <repo>\Chromerrunner\
  $scriptPath = $PSCommandPath
  if ([string]::IsNullOrWhiteSpace($scriptPath)) {
    $scriptPath = $MyInvocation.MyCommand.Path
  }
  if ([string]::IsNullOrWhiteSpace($scriptPath)) {
    throw "Cannot resolve script path (PSCommandPath/MyInvocation missing)."
  }
  $here = Split-Path -Parent $scriptPath
  return (Resolve-Path (Join-Path $here "..")).Path
}

function Get-OracleServerInfo {
  param([string]$RepoRoot)
  $py = Get-Command py -ErrorAction SilentlyContinue
  if (-not $py) { throw "py launcher not found. Install Python or ensure 'py -3' works." }

  # IMPORTANT: don't pass Python code via -c with embedded quotes on Windows.
  # PowerShell/Win32 quoting rules can strip quotes inside the argument.
  # Instead, write a temp .py file and execute it.
  $code = @'
import sys
from pathlib import Path
repo_root = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(repo_root))
from mirror_world_config import load_oracle_servers, resolve_oracle_ssh_key_path

servers, _ = load_oracle_servers(repo_root)
s = servers[0]
key = resolve_oracle_ssh_key_path(s["key"], repo_root)

print(s["user"])
print(s["host"])
print(str(key))
print(s.get("remote_root", "/home/rsadmin/bots/mirror-world"))
'@

  $tmpPy = Join-Path $env:TEMP ("oracle_gui_runner_info_{0}.py" -f ([guid]::NewGuid().ToString("N")))
  try {
    [System.IO.File]::WriteAllText($tmpPy, $code, [System.Text.Encoding]::UTF8)
    $lines = & py -3 $tmpPy $RepoRoot
  } finally {
    Remove-Item -LiteralPath $tmpPy -ErrorAction SilentlyContinue
  }
  if ($LASTEXITCODE -ne 0) { throw "Failed to load oraclekeys/servers.json via py -3" }
  if ($lines.Count -lt 4) { throw "Unexpected oracle server info output" }

  return [pscustomobject]@{
    User = $lines[0].Trim()
    Host = $lines[1].Trim()
    KeyPath = $lines[2].Trim()
    RemoteRoot = $lines[3].Trim()
  }
}

function Run-Ssh {
  param(
    [pscustomobject]$S,
    [string]$RemoteCmd,
    [int]$ConnectTimeout = 60
  )
  $ssh = "$env:WINDIR\System32\OpenSSH\ssh.exe"
  $wrapped = "bash -lc " + (Bash-SingleQuote $RemoteCmd)
  $args = @(
    "-i", $S.KeyPath,
    "-o", "StrictHostKeyChecking=no",
    "-o", "ServerAliveInterval=60",
    "-o", "ConnectTimeout=$ConnectTimeout",
    "$($S.User)@$($S.Host)",
    $wrapped
  )
  if ($Debug) { Write-Host "`nSSH $ssh $($args -join ' ')" }
  # IMPORTANT (Windows PowerShell 5.1):
  # `ssh.exe 2>&1 | ...` turns stderr into ErrorRecords and can surface as a scary "NativeCommandError"
  # even when ssh exits 0 (Playwright/Node deprecation warnings do this).
  # Redirect to temp files and print them as plain text instead.
  $out = Join-Path $env:TEMP ("oracle_gui_runner_ssh_out_{0}.txt" -f ([guid]::NewGuid().ToString("N")))
  $err = Join-Path $env:TEMP ("oracle_gui_runner_ssh_err_{0}.txt" -f ([guid]::NewGuid().ToString("N")))
  try {
    $p = Start-Process -FilePath $ssh -ArgumentList $args -NoNewWindow -Wait -PassThru `
      -RedirectStandardOutput $out -RedirectStandardError $err
    if (Test-Path $out) { Get-Content -LiteralPath $out | ForEach-Object { Write-Host $_ } }
    if (Test-Path $err) { Get-Content -LiteralPath $err | ForEach-Object { Write-Host $_ } }
    return [int]$p.ExitCode
  } finally {
    Remove-Item -LiteralPath $out -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $err -ErrorAction SilentlyContinue
  }
}

function Start-Tunnels {
  param([pscustomobject]$S)
  $ssh = "$env:WINDIR\System32\OpenSSH\ssh.exe"
  $args = @(
    "-i", $S.KeyPath,
    "-o", "StrictHostKeyChecking=no",
    "-o", "ServerAliveInterval=60",
    "-o", "ConnectTimeout=60",
    "-L", "6080:127.0.0.1:6080",
    "-L", "9222:127.0.0.1:9222",
    "$($S.User)@$($S.Host)"
  )
  Write-Host ""
  Write-Host "Opening tunnels in a new window. Keep it open."
  Write-Host "- noVNC: http://127.0.0.1:6080/vnc.html"
  Write-Host "- CDP:   http://127.0.0.1:9222/json/version"
  Start-Process -FilePath $ssh -ArgumentList $args -WindowStyle Normal
}

function Open-LocalNovnc {
  $novnc = "http://127.0.0.1:6080/vnc.html"
  Write-Host ""
  Write-Host "Opening noVNC in your default local browser..."
  Write-Host ("URL: {0}" -f $novnc)
  Start-Process $novnc | Out-Null
}

function Prompt-Url {
  $u = Read-Host "Paste product URL (blank to cancel)"
  if ([string]::IsNullOrWhiteSpace($u)) { return "" }
  return $u.Trim()
}

function Menu {
  param([pscustomobject]$S)

  while ($true) {
    Write-Header "Chromerrunner Oracle GUI/CDP Runner"
    Write-Host ("Server: {0}@{1}" -f $S.User, $S.Host)
    Write-Host ("Key:    {0}" -f $S.KeyPath)
    Write-Host ("Root:   {0}" -f $S.RemoteRoot)
    Write-Host ""
    Write-Host "1) Start/verify noVNC on Oracle (port 6080)"
    Write-Host "2) Open tunnels (noVNC 6080 + CDP 9222)"
    Write-Host "3) Start Oracle Chrome CDP (HEADED) [shows in noVNC]"
    Write-Host "4) Start Oracle Chrome CDP (HEADLESS)"
    Write-Host "5) Run Generic Checker (CDP + MANUAL ENTER) [paste URL]"
    Write-Host "6) One-shot: noVNC + tunnels + headed chrome + run checker"
    Write-Host "0) Exit"
    Write-Host ""

    $raw = Read-Host "Selection"
    if ($null -eq $raw) { return }
    $ch = $raw.Trim()
    switch ($ch) {
      "0" { return }
      "1" {
        $rc = Run-Ssh $S ("cd {0}/Chromerrunner && chmod +x start_oracle_novnc.sh && bash start_oracle_novnc.sh" -f $S.RemoteRoot)
        if ($rc -ne 0) { Write-Host "ERROR: noVNC start failed (exit=$rc)" }
        Pause
      }
      "2" {
        Start-Tunnels $S
        Open-LocalNovnc
        Pause
      }
      "3" {
        $rc = Run-Ssh $S ("cd {0}/Chromerrunner && chmod +x start_chrome_oracle_cdp.sh && unset DISPLAY; nohup env CHROME_BIN=/opt/google/chrome/google-chrome bash ./start_chrome_oracle_cdp.sh --headed >/tmp/chromerrunner_cdp_chrome.log 2>&1 & sleep 2; curl -s http://127.0.0.1:9222/json/version | head -c 240; echo" -f $S.RemoteRoot)
        if ($rc -ne 0) { Write-Host "ERROR: Chrome start failed (exit=$rc)" }
        Write-Host ""
        Write-Host "Note: CDP JSON User-Agent strings are not a perfect signal on Linux. Use noVNC to confirm you see a real Chrome window."
        Pause
      }
      "4" {
        $rc = Run-Ssh $S ("cd {0}/Chromerrunner && chmod +x start_chrome_oracle_cdp.sh && nohup bash start_chrome_oracle_cdp.sh >/tmp/chromerrunner_cdp_chrome.log 2>&1 & sleep 1; curl -s http://127.0.0.1:9222/json/version | head -c 200; echo" -f $S.RemoteRoot)
        if ($rc -ne 0) { Write-Host "ERROR: Chrome start failed (exit=$rc)" }
        Pause
      }
      "5" {
        $url = Prompt-Url
        if (-not $url) { continue }
        Write-Host ""
        Write-Host "Run flow:"
        Write-Host "- In noVNC: click the Chrome window, load the URL, and complete any human checks (Walmart 'Robot or human?', etc.)."
        Write-Host "- Wait until the normal product page is visible (not the bot interstitial)."
        Write-Host "- Then come back here: the SSH session will prompt you to press ENTER to extract."
        Write-Host ""
        $rc = Run-Ssh $S ("cd {0}/Chromerrunner && export NODE_NO_WARNINGS=1 NODE_OPTIONS=--no-deprecation && source .venv/bin/activate && python generic_product_checker.py --url '{1}' --connect-cdp --cdp-url http://127.0.0.1:9222 --manual" -f $S.RemoteRoot, $url)
        if ($rc -ne 0) { Write-Host "ERROR: checker failed (exit=$rc)" }
        Pause
      }
      "6" {
        $null = Run-Ssh $S ("cd {0}/Chromerrunner && chmod +x start_oracle_novnc.sh && bash start_oracle_novnc.sh" -f $S.RemoteRoot)
        Start-Tunnels $S
        Open-LocalNovnc
        $null = Run-Ssh $S ("cd {0}/Chromerrunner && chmod +x start_chrome_oracle_cdp.sh && unset DISPLAY; nohup env CHROME_BIN=/opt/google/chrome/google-chrome bash ./start_chrome_oracle_cdp.sh --headed >/tmp/chromerrunner_cdp_chrome.log 2>&1 & sleep 2; curl -s http://127.0.0.1:9222/json/version | head -c 240; echo" -f $S.RemoteRoot)
        Write-Host ""
        Write-Host "Note: CDP JSON User-Agent strings are not a perfect signal on Linux. Use noVNC to confirm you see a real Chrome window."
        $url = Prompt-Url
        if (-not $url) { continue }
        Write-Host ""
        Write-Host "Next: complete any bot checks in noVNC (Walmart/GameStop), then press ENTER in the SSH checker prompt."
        $null = Run-Ssh $S ("cd {0}/Chromerrunner && export NODE_NO_WARNINGS=1 NODE_OPTIONS=--no-deprecation && source .venv/bin/activate && python generic_product_checker.py --url '{1}' --connect-cdp --cdp-url http://127.0.0.1:9222 --manual" -f $S.RemoteRoot, $url)
        Pause
      }
      default {
        continue
      }
    }
  }
}

$repoRoot = Resolve-RepoRoot
$server = Get-OracleServerInfo -RepoRoot $repoRoot
Menu -S $server

