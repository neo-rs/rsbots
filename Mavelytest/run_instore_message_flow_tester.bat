@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0.."
set PYTHONUTF8=1
chcp 65001 >nul 2>&1

title Instore message flow tester

echo.
echo ============================================================
echo   Instorebotforwarder - message flow tester
echo   Repo: %cd%
echo   Dry-run by default. Use Live send = Y to really post.
echo   Stop the live Instore service if using the same bot token.
echo   After each run you can start another test without closing.
echo ============================================================
echo.

set "CLI_LINK_USED="
set "LAST_EC=0"

:main_cycle
if not "%~1"=="" if not defined CLI_LINK_USED goto have_arg1
goto after_arg1
:have_arg1
echo %~1| findstr /i "discord.com/channels/ discordapp.com/channels/ ptb.discord.com/channels/ canary.discord.com/channels/" >nul
if not errorlevel 1 goto use_arg1_as_link
echo [skip] First argument is not a Discord message link:
echo        %~1
echo        Tip: use Copy Message Link in Discord, or run without args.
echo.
goto after_arg1
:use_arg1_as_link
set "INSTORE_FLOW_TEST_LINK=%~1"
set "CLI_LINK_USED=1"
echo Using link from command line.
echo   !INSTORE_FLOW_TEST_LINK!
echo.
goto live_choice
:after_arg1

:ask_link
echo ------------------------------------------------------------
echo   Discord message link
echo ------------------------------------------------------------
echo   In Discord: right-click the message - Copy Message Link
echo   One cmd.exe prompt below - paste the link and press Enter.
echo   Ampersands in URLs are fine ^(paste whole line^). Not a Windows path ^(C:\...^).
echo.
set "DISCORD_MSG_LINK="
set /p "DISCORD_MSG_LINK=   Paste link: "

if not defined DISCORD_MSG_LINK goto ask_link_empty
if "!DISCORD_MSG_LINK!"=="" goto ask_link_empty

set "T=!DISCORD_MSG_LINK!"
if "!T:~1,1!"==":" if "!T:~2,1!"=="\" goto reject_windows_path

set "VCHK=%TEMP%\instore_msglink_%RANDOM%.tmp"
del "!VCHK!" 2>nul
rem Write link to file so findstr never pipes user text ^(avoids FINDSTR errors and ^& breaking the pipe^)
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$p = $env:VCHK; $s = [Environment]::GetEnvironmentVariable('DISCORD_MSG_LINK','Process'); if ($null -eq $s) { $s = '' }; [IO.File]::WriteAllText($p, $s, [Text.UTF8Encoding]::new($false))"

if not exist "!VCHK!" goto ask_link_empty

findstr /i /m /c:"discord.com/channels/" /c:"discordapp.com/channels/" /c:"ptb.discord.com/channels/" /c:"canary.discord.com/channels/" "!VCHK!" >nul
if errorlevel 1 (
  del "!VCHK!" 2>nul
  set "VCHK="
  goto reject_not_discord
)

powershell -NoProfile -Command "exit ([int]([IO.File]::ReadAllText($env:VCHK).Contains([char]92)))"
if errorlevel 1 (
  del "!VCHK!" 2>nul
  set "VCHK="
  goto reject_backslash
)

del "!VCHK!" 2>nul
set "VCHK="
set "INSTORE_FLOW_TEST_LINK=!DISCORD_MSG_LINK!"
goto live_choice

:ask_link_empty
echo.
echo   Empty input - copy the message link from Discord ^(https://discord.com/channels/...^) and paste again.
echo.
goto ask_link

:reject_windows_path
echo.
echo   That looks like a Windows file path, not a Discord message link.
echo   In Discord: right-click the message - Copy Message Link ^(starts with https://^).
echo.
goto ask_link

:reject_not_discord
echo.
echo   That does not look like a Discord message link.
echo   Expected: https://discord.com/channels/GUILD_ID/CHANNEL_ID/MESSAGE_ID
echo   Also ok: ptb.discord.com or canary.discord.com same path shape.
echo.
goto ask_link

:reject_backslash
echo.
echo   That contains backslashes - use the https link from Discord, not a file path.
echo.
goto ask_link

:live_choice
if not defined INSTORE_FLOW_TEST_LINK goto reject_no_link
goto after_link_ready

:reject_no_link
echo Nothing to run.
goto ask_link

:after_link_ready
echo.
echo ------------------------------------------------------------
echo   Live send to Discord destinations?
echo ------------------------------------------------------------
choice /c YN /n /m "   Y = live post, N = dry-run only : "
if errorlevel 2 (
  set "EXTRA="
) else (
  set "EXTRA=--live-send"
)

echo.
echo Running python with link in env INSTORE_FLOW_TEST_LINK ...
echo.
python Mavelytest\instore_message_flow_tester.py !EXTRA!
set LAST_EC=!ERRORLEVEL!
echo.
if !LAST_EC! neq 0 echo Exit code: !LAST_EC!

echo.
echo ------------------------------------------------------------
choice /c YN /n /m "   Run another message test? Y = yes, N = exit : "
if errorlevel 2 goto all_done

set "INSTORE_FLOW_TEST_LINK="
set "DISCORD_MSG_LINK="
set "EXTRA="
echo.
goto main_cycle

:all_done
exit /b !LAST_EC!
