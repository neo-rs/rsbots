#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Network Bandwidth Monitor
Tracks network usage (bytes sent/received) for bots both locally and remotely
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

try:
    import psutil
except ImportError:
    print("[ERROR] psutil not installed. Install with: pip install psutil")
    sys.exit(1)

# For remote monitoring
try:
    from oraclekeys.ssh_utils import load_servers, CONFIG_NAME, build_ssh_base
    from oraclekeys.ssh_terminal import execute_remote_command
except ImportError:
    print("[WARN] Could not import SSH utilities. Remote monitoring disabled.")


class NetworkBandwidthMonitor:
    """Monitor network bandwidth usage"""
    
    def __init__(self):
        self.project_root = project_root
        self.bot_scripts = {
            "testcenter": "testcenter_bot.py",
            "datamanagerbot": "datamanagerbot.py",
            "discumbot": "discumbot.py",
            "pingbot": "pingbot.py",
        }
    
    def get_local_network_usage(self) -> Dict[str, Any]:
        """Get network usage for local bot processes"""
        stats = {
            "total_bytes_sent": 0,
            "total_bytes_recv": 0,
            "by_bot": {},
            "by_interface": {},
        }
        
        # Get overall network stats by interface
        net_io = psutil.net_io_counters(pernic=True)
        for interface, io in net_io.items():
            stats["by_interface"][interface] = {
                "bytes_sent": io.bytes_sent,
                "bytes_recv": io.bytes_recv,
            }
            stats["total_bytes_sent"] += io.bytes_sent
            stats["total_bytes_recv"] += io.bytes_recv
        
        # Get network usage per bot process
        matching_procs = {}
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.cmdline()
                if not cmdline:
                    continue
                
                cmdline_str = ' '.join(cmdline).lower()
                
                # Check which bot this process belongs to
                for bot_key, script_name in self.bot_scripts.items():
                    if script_name.lower() in cmdline_str and bot_key not in matching_procs:
                        matching_procs[bot_key] = proc
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Get network I/O for each bot process
        for bot_key, proc in matching_procs.items():
            try:
                # Note: psutil doesn't directly support per-process network I/O on all platforms
                # On Linux, we can use /proc/<pid>/net/sockstat or netstat
                # For now, we'll try to get connection info
                connections = proc.connections()
                
                stats["by_bot"][bot_key] = {
                    "pid": proc.pid,
                    "connections": len(connections),
                    "connection_details": [
                        {
                            "status": conn.status,
                            "local": f"{conn.laddr.ip}:{conn.laddr.port}" if conn.laddr else "N/A",
                            "remote": f"{conn.raddr.ip}:{conn.raddr.port}" if conn.raddr else "N/A",
                        }
                        for conn in connections[:10]  # Limit to 10 connections
                    ]
                }
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
            except Exception as e:
                # Some platforms don't support per-process network stats
                stats["by_bot"][bot_key] = {
                    "pid": proc.pid,
                    "error": f"Network stats not available: {e}",
                }
        
        return stats
    
    def get_remote_network_usage(self, server_entry: Dict[str, Any]) -> Dict[str, Any]:
        """Get network usage from remote Oracle Ubuntu server"""
        stats = {
            "total_bytes_sent": 0,
            "total_bytes_recv": 0,
            "by_interface": {},
            "by_bot": {},
            "error": None,
        }
        
        base_ssh_cmd = build_ssh_base(server_entry)
        
        def _run_remote_cmd(cmd: str, timeout: int = 10) -> str:
            full_cmd = f'{base_ssh_cmd} "{cmd}"'
            try:
                import subprocess
                result = subprocess.run(
                    full_cmd, shell=True, capture_output=True, text=True, timeout=timeout,
                    encoding='utf-8', errors='replace'
                )
                if result.returncode == 0:
                    return result.stdout.strip()
                else:
                    return f"Error (exit {result.returncode}): {result.stderr.strip()}"
            except subprocess.TimeoutExpired:
                return "Timeout"
            except Exception as e:
                return f"Exception: {e}"
        
        try:
            # Get overall network stats
            # Use ifconfig or ip command
            net_stats = _run_remote_cmd("cat /proc/net/dev")
            if "Error" not in net_stats and "Timeout" not in net_stats:
                # Parse /proc/net/dev
                for line in net_stats.split('\n')[2:]:  # Skip header lines
                    parts = line.split()
                    if len(parts) >= 10:
                        interface = parts[0].rstrip(':')
                        bytes_recv = int(parts[1])
                        bytes_sent = int(parts[9])
                        
                        stats["by_interface"][interface] = {
                            "bytes_sent": bytes_sent,
                            "bytes_recv": bytes_recv,
                        }
                        stats["total_bytes_sent"] += bytes_sent
                        stats["total_bytes_recv"] += bytes_recv
            
            # Get network usage per bot process
            # Use ss or netstat to find connections per process
            for bot_key, script_name in self.bot_scripts.items():
                # Find PIDs of bot processes
                pid_cmd = f"pgrep -f '{script_name}' | head -1"
                pid = _run_remote_cmd(pid_cmd)
                
                if pid and pid.isdigit():
                    pid = int(pid)
                    # Get network connections for this PID
                    conn_cmd = f"ss -tnp 2>/dev/null | grep 'pid={pid}' | wc -l"
                    conn_count = _run_remote_cmd(conn_cmd)
                    
                    # Get network I/O for this PID (if available)
                    # On Linux, we can check /proc/<pid>/net/sockstat
                    sockstat_cmd = f"cat /proc/{pid}/net/sockstat 2>/dev/null || echo 'N/A'"
                    sockstat = _run_remote_cmd(sockstat_cmd)
                    
                    stats["by_bot"][bot_key] = {
                        "pid": pid,
                        "connections": int(conn_count) if conn_count.isdigit() else 0,
                        "sockstat": sockstat if "N/A" not in sockstat else None,
                    }
                else:
                    stats["by_bot"][bot_key] = {
                        "pid": None,
                        "status": "not_running",
                    }
        
        except Exception as e:
            stats["error"] = str(e)
        
        return stats
    
    def format_bytes(self, bytes_val: int) -> str:
        """Format bytes to human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
    
    def print_report(self, local_stats: Dict[str, Any], remote_stats: Optional[Dict[str, Any]] = None):
        """Print formatted network usage report"""
        print("="*70)
        print("NETWORK BANDWIDTH USAGE REPORT")
        print("="*70)
        
        # Local stats
        print("\n[LOCAL NETWORK USAGE]")
        print("-"*70)
        print(f"Total Bytes Sent: {self.format_bytes(local_stats.get('total_bytes_sent', 0))}")
        print(f"Total Bytes Received: {self.format_bytes(local_stats.get('total_bytes_recv', 0))}")
        
        if local_stats.get('by_interface'):
            print("\nBy Interface:")
            for interface, io in local_stats['by_interface'].items():
                sent = self.format_bytes(io['bytes_sent'])
                recv = self.format_bytes(io['bytes_recv'])
                print(f"  {interface:15} Sent: {sent:>12} | Recv: {recv:>12}")
        
        if local_stats.get('by_bot'):
            print("\nBy Bot (Local):")
            for bot, info in local_stats['by_bot'].items():
                pid = info.get('pid', '?')
                conns = info.get('connections', 0)
                print(f"  {bot:20} PID {pid:>6} | Connections: {conns:>3}")
                if 'connection_details' in info and info['connection_details']:
                    for conn in info['connection_details'][:3]:  # Show first 3
                        print(f"    -> {conn.get('status', 'N/A'):10} {conn.get('remote', 'N/A')}")
        
        # Remote stats
        if remote_stats:
            print("\n[REMOTE NETWORK USAGE (Oracle Ubuntu)]")
            print("-"*70)
            if remote_stats.get('error'):
                print(f"  [ERROR] {remote_stats['error']}")
            else:
                print(f"Total Bytes Sent: {self.format_bytes(remote_stats.get('total_bytes_sent', 0))}")
                print(f"Total Bytes Received: {self.format_bytes(remote_stats.get('total_bytes_recv', 0))}")
                
                if remote_stats.get('by_interface'):
                    print("\nBy Interface:")
                    for interface, io in remote_stats['by_interface'].items():
                        sent = self.format_bytes(io['bytes_sent'])
                        recv = self.format_bytes(io['bytes_recv'])
                        print(f"  {interface:15} Sent: {sent:>12} | Recv: {recv:>12}")
                
                if remote_stats.get('by_bot'):
                    print("\nBy Bot (Remote):")
                    for bot, info in remote_stats['by_bot'].items():
                        pid = info.get('pid', '?')
                        conns = info.get('connections', 0)
                        status = info.get('status', 'running')
                        print(f"  {bot:20} PID {pid:>6} | Connections: {conns:>3} | Status: {status}")
        
        print("\n" + "="*70)
        print("Note: Network usage is cumulative since system boot.")
        print("For per-bot bandwidth, check connection counts and interface stats.")
        print("="*70)


def main():
    """Main function"""
    monitor = NetworkBandwidthMonitor()
    
    # Get local stats
    print("Collecting local network usage...")
    local_stats = monitor.get_local_network_usage()
    
    # Get remote stats (if available)
    remote_stats = None
    try:
        script_dir = Path(__file__).parent.parent / "oraclekeys"
        cfg_path = script_dir / CONFIG_NAME
        if cfg_path.exists():
            servers = load_servers(cfg_path)
            if servers:
                print("Collecting remote network usage from Oracle Ubuntu...")
                remote_stats = monitor.get_remote_network_usage(servers[0])
    except Exception as e:
        print(f"[WARN] Could not get remote stats: {e}")
    
    # Print report
    monitor.print_report(local_stats, remote_stats)
    
    # Save report
    report = {
        "timestamp": datetime.now().isoformat(),
        "local": local_stats,
        "remote": remote_stats,
    }
    
    report_file = project_root / "logs" / "network_bandwidth_report.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n[OK] Report saved to: {report_file}")


if __name__ == "__main__":
    main()









