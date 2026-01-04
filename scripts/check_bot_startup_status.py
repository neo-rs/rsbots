#!/usr/bin/env python3
"""
Helper script to check if bots have logged their startup status.
Called from restart.bat to avoid complex inline Python.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from neonxt.core.unified_bot_runner import get_bot_status

def check_startup_logs():
    """Check which bots have logged startup status."""
    bots = get_bot_status(None)
    prefixes = {
        'testcenter': 'TESTCENTER',
        'datamanagerbot': 'DATAMANAGER',
        'discumbot': 'DISCUMBOT',
        'pingbot': 'PINGBOT'
    }
    
    results = {}
    for name, info in bots.items():
        log_path = project_root / 'logs' / f'unified_runner_{name}.log'
        has_log = False
        
        if log_path.exists():
            try:
                # Read last 10KB of log
                content = log_path.read_text(encoding='utf-8', errors='ignore')[-10000:]
                prefix = prefixes.get(name, '')
                has_log = (
                    prefix in content or 
                    'STARTUP STATUS' in content
                )
            except Exception:
                pass
        
        results[name] = {
            'running': info.get('running', False),
            'has_log': has_log
        }
    
    return results

if __name__ == '__main__':
    results = check_startup_logs()
    
    # Count running and logged
    running_count = sum(1 for r in results.values() if r['running'])
    logged_count = sum(1 for r in results.values() if r['has_log'])
    total = len(results)
    
    # Print status
    print(f'Processes: {running_count}/{total} running')
    print(f'Startup Logs: {logged_count}/{total} bots logged status')
    
    # Print individual status
    for name, info in results.items():
        status = 'RUNNING' if info['running'] else 'STOPPED'
        log_status = 'HAS LOG' if info['has_log'] else 'NO LOG'
        print(f'  {name}: {status} - {log_status}')
    
    # Exit code: 0 if all running and logged, 1 otherwise
    all_ready = (
        all(r['running'] for r in results.values()) and
        all(r['has_log'] for r in results.values())
    )
    sys.exit(0 if all_ready else 1)

