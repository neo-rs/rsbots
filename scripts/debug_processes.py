#!/usr/bin/env python3
import psutil

print("Checking known bot PIDs...")
print("="*70)

# PIDs from the status output
known_pids = [16432, 7836, 9728, 2892]

for pid in known_pids:
    try:
        proc = psutil.Process(pid)
        print(f"\nPID {pid}:")
        print(f"  Name: {proc.name()}")
        print(f"  Cmdline: {' '.join(proc.cmdline())}")
        print(f"  CPU: {proc.cpu_percent(interval=0.1):.1f}%")
        print(f"  Memory: {proc.memory_info().rss / 1024 / 1024:.1f} MB")
    except psutil.NoSuchProcess:
        print(f"\nPID {pid}: Process not found")
    except Exception as e:
        print(f"\nPID {pid}: Error - {e}")

print("\n" + "="*70)
print("\nSearching all Python processes...")
print("="*70)

for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
    try:
        if 'python' in proc.name().lower():
            cmdline = proc.cmdline()
            if cmdline and len(cmdline) > 1:
                script = cmdline[-1] if cmdline else ''
                if 'bot' in script.lower() or 'neonxt' in ' '.join(cmdline).lower():
                    print(f"\nPID {proc.pid}: {script}")
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
