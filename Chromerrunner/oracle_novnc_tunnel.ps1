# One-shot: start Oracle noVNC for CDP Chrome + SSH tunnels + open local browser.
param(
  [switch]$NoBrowser,
  [switch]$SkipUpload,
  [switch]$SkipChromeClean,
  [switch]$Stop,
  [switch]$CleanChrome,
  [switch]$Debug
)

$ErrorActionPreference = "Stop"

function Bash-SingleQuote {
  param([string]$Text)
  $rep = "'" + '"' + "'" + '"' + "'"
  return "'" + ($Text -replace "'", $rep) + "'"
}

function Resolve-RepoRoot {
  $scriptPath = $PSCommandPath
  if ([string]::IsNullOrWhiteSpace($scriptPath)) {
    $scriptPath = $MyInvocation.MyCommand.Path
  }
  $here = Split-Path -Parent $scriptPath
  return (Resolve-Path (Join-Path $here "..")).Path
}

function Get-OracleServerInfo {
  param([string]$RepoRoot)
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
  $tmpPy = Join-Path $env:TEMP ("oracle_novnc_tunnel_{0}.py" -f ([guid]::NewGuid().ToString("N")))
  try {
    [System.IO.File]::WriteAllText($tmpPy, $code, [System.Text.Encoding]::UTF8)
    $lines = & py -3 $tmpPy $RepoRoot
  } finally {
    Remove-Item -LiteralPath $tmpPy -ErrorAction SilentlyContinue
  }
  if ($LASTEXITCODE -ne 0) { throw "Failed to load oraclekeys/servers.json" }
  return [pscustomobject]@{
    User       = $lines[0].Trim()
    Host       = $lines[1].Trim()
    KeyPath    = $lines[2].Trim()
    RemoteRoot = $lines[3].Trim()
  }
}

function Invoke-Ssh {
  param(
    [pscustomobject]$S,
    [string]$RemoteCmd,
    [int]$ConnectTimeout = 90
  )
  $ssh = "$env:WINDIR\System32\OpenSSH\ssh.exe"
  $wrapped = "bash -lc " + (Bash-SingleQuote $RemoteCmd)
  $args = @(
    "-i", $S.KeyPath,
    "-o", "StrictHostKeyChecking=no",
    "-o", "ServerAliveInterval=60",
    "-o", "ConnectTimeout=$ConnectTimeout",
    ($S.User + "@" + $S.Host),
    $wrapped
  )
  if ($Debug) { Write-Host "SSH: $ssh $($args -join ' ')" }
  $out = Join-Path $env:TEMP ("oracle_novnc_ssh_out_{0}.txt" -f ([guid]::NewGuid().ToString("N")))
  $err = Join-Path $env:TEMP ("oracle_novnc_ssh_err_{0}.txt" -f ([guid]::NewGuid().ToString("N")))
  try {
    $p = Start-Process -FilePath $ssh -ArgumentList $args -NoNewWindow -Wait -PassThru `
      -RedirectStandardOutput $out -RedirectStandardError $err
    if (Test-Path $out) { Get-Content -LiteralPath $out | ForEach-Object { Write-Host $_ } }
    if (Test-Path $err) { Get-Content -LiteralPath $err | ForEach-Object { Write-Host $_ } }
    return [int]$p.ExitCode
  } finally {
    Remove-Item -LiteralPath $out, $err -ErrorAction SilentlyContinue
  }
}

function Test-LocalPort {
  param([int]$Port)
  try {
    $client = New-Object System.Net.Sockets.TcpClient
    $iar = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
    $ok = $iar.AsyncWaitHandle.WaitOne(1500, $false)
    if (-not $ok) { $client.Close(); return $false }
    $client.EndConnect($iar) | Out-Null
    $client.Close()
    return $true
  } catch {
    return $false
  }
}

function Get-TunnelPidPath {
  return (Join-Path $env:TEMP "oracle_novnc_tunnel_ssh.pid")
}

function Get-LocalListenPid {
  param([int]$Port)
  try {
    $line = netstat -ano | Select-String -Pattern "127\.0\.0\.1:$Port\s+.*LISTENING\s+(\d+)" | Select-Object -First 1
    if ($line -match '\s+(\d+)\s*$') { return [int]$Matches[1] }
  } catch { }
  return 0
}

function Stop-ExistingTunnel {
  $pidFile = Get-TunnelPidPath
  if (Test-Path -LiteralPath $pidFile) {
    $oldPid = (Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($oldPid -match '^\d+$') {
      $proc = Get-Process -Id ([int]$oldPid) -ErrorAction SilentlyContinue
      if ($proc) {
        Write-Host "Stopping previous tunnel (PID $oldPid)..."
        Stop-Process -Id ([int]$oldPid) -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
      }
    }
    Remove-Item -LiteralPath $pidFile -ErrorAction SilentlyContinue
  }
  foreach ($port in @(6080, 9222)) {
    $lpid = Get-LocalListenPid -Port $port
    if ($lpid -le 0) { continue }
    $p = Get-Process -Id $lpid -ErrorAction SilentlyContinue
    if ($p -and $p.ProcessName -match 'ssh') {
      Write-Host "Stopping ssh.exe still listening on port $port (PID $lpid)..."
      Stop-Process -Id $lpid -Force -ErrorAction SilentlyContinue
      Start-Sleep -Seconds 1
    }
  }
}

function Start-SshTunnels {
  param([pscustomobject]$S)
  $ssh = "$env:WINDIR\System32\OpenSSH\ssh.exe"
  if (-not (Test-Path -LiteralPath $ssh)) { throw "OpenSSH not found: $ssh" }
  if (-not (Test-Path -LiteralPath $S.KeyPath)) { throw "SSH key not found: $($S.KeyPath)" }

  Stop-ExistingTunnel

  # -N = port-forward only (stay alive). Direct Start-Process + ArgumentList is reliable on Windows.
  $sshArgs = @(
    "-N",
    "-T",
    "-i", $S.KeyPath,
    "-o", "StrictHostKeyChecking=no",
    "-o", "ServerAliveInterval=60",
    "-o", "ConnectTimeout=60",
    "-L", "6080:127.0.0.1:6080",
    "-L", "9222:127.0.0.1:9222",
    ($S.User + "@" + $S.Host)
  )

  Write-Host ""
  Write-Host "Starting SSH tunnel (background ssh.exe):"
  Write-Host "  noVNC  -> http://127.0.0.1:6080/vnc.html"
  Write-Host "  CDP    -> http://127.0.0.1:9222/json/version"

  $errLog = Join-Path $env:TEMP "oracle_novnc_tunnel_ssh.err"
  Remove-Item -LiteralPath $errLog -ErrorAction SilentlyContinue
  $proc = Start-Process -FilePath $ssh -ArgumentList $sshArgs -PassThru -WindowStyle Hidden `
    -RedirectStandardError $errLog
  if (-not $proc) { throw "Failed to start ssh.exe" }
  Start-Sleep -Seconds 2
  $alive = Get-Process -Id $proc.Id -ErrorAction SilentlyContinue
  if (-not $alive) {
    $msg = ""
    if (Test-Path -LiteralPath $errLog) { $msg = (Get-Content -LiteralPath $errLog -Raw).Trim() }
    throw "SSH tunnel exited immediately. $msg"
  }
  Set-Content -LiteralPath (Get-TunnelPidPath) -Value $proc.Id -Encoding ASCII
  Write-Host "Tunnel PID: $($proc.Id)  (run oracle_novnc_tunnel_stop.bat to close)"
}

function Wait-LocalTunnel {
  param([int]$Port = 6080, [int]$TimeoutSec = 45)
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    if (Test-LocalPort -Port $Port) { return $true }
    Start-Sleep -Milliseconds 500
  }
  return $false
}

if ($Stop) {
  Stop-ExistingTunnel
  Write-Host "SSH tunnel stopped."
  exit 0
}

$repoRoot = Resolve-RepoRoot
$S = Get-OracleServerInfo -RepoRoot $repoRoot
$chromDir = ($S.RemoteRoot.TrimEnd('/') + '/Chromerrunner')

if ($CleanChrome) {
  Write-Host "Clean restart: single CDP Chrome on Oracle (closes stacked windows)..."
  $localSh = Join-Path (Split-Path $PSCommandPath) "restart_cdp_chrome_clean.sh"
  $remoteSh = "$chromDir/restart_cdp_chrome_clean.sh"
  if (Test-Path -LiteralPath $localSh) {
    $scp = "$env:WINDIR\System32\OpenSSH\scp.exe"
    $scpTarget = $S.User + "@" + $S.Host + ":" + $remoteSh
    $scpArgs = @("-i", $S.KeyPath, "-o", "StrictHostKeyChecking=no", $localSh, $scpTarget)
    $null = Start-Process -FilePath $scp -ArgumentList $scpArgs -NoNewWindow -Wait -PassThru
  }
  $cleanCmd = "cd $chromDir && chmod +x restart_cdp_chrome_clean.sh && bash ./restart_cdp_chrome_clean.sh"
  $null = Invoke-Ssh -S $S -RemoteCmd $cleanCmd
  exit 0
}

Write-Host "================================================================"
Write-Host " Oracle noVNC -> CDP Chrome (amazon/ebay profile)"
Write-Host (" Server: " + $S.User + "@" + $S.Host)
Write-Host "================================================================"

# Optional: upload small launcher scripts (~10 KB total). Does NOT touch the Chrome profile.
$chromRunnerDir = Split-Path $PSCommandPath
if (-not $SkipUpload) {
  Write-Host "Syncing Chromerrunner launcher scripts to Oracle (not the Chrome profile)..."
  $scp = "$env:WINDIR\System32\OpenSSH\scp.exe"
  foreach ($name in @("start_oracle_novnc_for_cdp.sh", "start_chrome_oracle_cdp.sh", "ensure_headed_cdp_chrome.sh", "restart_cdp_chrome_clean.sh")) {
    $localSh = Join-Path $chromRunnerDir $name
    $remoteSh = "$chromDir/$name"
    if (Test-Path -LiteralPath $localSh) {
      $scpTarget = $S.User + "@" + $S.Host + ":" + $remoteSh
      $scpArgs = @("-i", $S.KeyPath, "-o", "StrictHostKeyChecking=no", $localSh, $scpTarget)
      $null = Start-Process -FilePath $scp -ArgumentList $scpArgs -NoNewWindow -Wait -PassThru
    }
  }
} else {
  Write-Host "Skipping script upload (-SkipUpload)."
}

Write-Host ""
if ($SkipChromeClean) {
  Write-Host "[1/3] CDP Chrome (skip clean -SkipChromeClean)..."
  $cdpCmd = "cd $chromDir && chmod +x ensure_headed_cdp_chrome.sh && bash ./ensure_headed_cdp_chrome.sh"
} else {
  Write-Host "[1/3] CDP Chrome: check Oracle, clean duplicates, start one instance..."
  $cdpCmd = "cd $chromDir && chmod +x restart_cdp_chrome_clean.sh && bash ./restart_cdp_chrome_clean.sh"
}
$null = Invoke-Ssh -S $S -RemoteCmd $cdpCmd

Write-Host ""
Write-Host "[2/3] Start noVNC on Oracle (display :99 = CDP Chrome)..."
$novncCmd = "cd $chromDir && chmod +x start_oracle_novnc_for_cdp.sh && bash ./start_oracle_novnc_for_cdp.sh"
$rc = Invoke-Ssh -S $S -RemoteCmd $novncCmd
if ($rc -ne 0) {
  Write-Host "WARNING: noVNC start returned exit $rc (tunnel may still work if already running)"
}

Write-Host ""
Write-Host "[3/3] Open local SSH tunnels + browser..."
try {
  Start-SshTunnels -S $S
} catch {
  Write-Host $_.Exception.Message -ForegroundColor Red
  exit 1
}
Write-Host "Waiting for local port 6080 (SSH tunnel)..."
if (Wait-LocalTunnel -Port 6080 -TimeoutSec 30) {
  Write-Host "OK: local port 6080 is reachable."
} else {
  Write-Host ""
  Write-Host "ERROR: local port 6080 is not open yet." -ForegroundColor Red
  $pidFile = Get-TunnelPidPath
  if (Test-Path -LiteralPath $pidFile) {
    $tpid = Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue
    $alive = Get-Process -Id ([int]$tpid) -ErrorAction SilentlyContinue
    if (-not $alive) { Write-Host "SSH tunnel process already exited (PID $tpid). Test manually:" }
    else { Write-Host "SSH still running (PID $tpid) but port not listening yet." }
  }
  Write-Host "Manual test:"
  Write-Host ("  ssh -N -i `"$($S.KeyPath)`" -L 6080:127.0.0.1:6080 " + $S.User + "@" + $S.Host)
  Write-Host "Common fixes:"
  Write-Host "  - Close anything else using local port 6080 or 9222"
  Write-Host "  - Run oracle_novnc_tunnel_stop.bat then retry"
  if (-not $NoBrowser) {
    Write-Host ""
    Write-Host "Not opening browser until tunnel works. Fix tunnel, then open:"
    Write-Host "  http://127.0.0.1:6080/vnc.html"
    exit 1
  }
}

if (-not $NoBrowser) {
  $url = "http://127.0.0.1:6080/vnc.html"
  Write-Host "Opening $url"
  Start-Process $url | Out-Null
}

Write-Host ""
Write-Host "Done. In noVNC you should see the CDP Chrome window (oracle_real_chrome_profile)."
Write-Host "Site warmup (same profile Instorebotforwarder uses via CDP :9222):"
Write-Host "  - eBay / Amazon buybox: already wired in config (ebay_first8_connect_cdp, amazon_buybox_connect_cdp)"
Write-Host "  - Other retailers (Walmart, GameStop, etc.): open the site here and pass any human/bot check"
Write-Host "  Cookies persist in oracle_real_chrome_profile for all CDP-attached scrapes."
Write-Host "Close this window when done - oracle_novnc_tunnel.bat will stop the SSH tunnel on keypress."
