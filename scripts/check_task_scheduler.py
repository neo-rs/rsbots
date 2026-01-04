#!/usr/bin/env python3
"""
Check Windows Task Scheduler for RS Forwarder bot entries.
"""
import subprocess
import sys

def check_task_scheduler():
    """Check for scheduled tasks related to RS forwarder."""
    print("Checking Windows Task Scheduler for RS Forwarder entries...")
    print()
    
    try:
        # List all tasks
        result = subprocess.run(
            ['schtasks', '/query', '/fo', 'LIST', '/v'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            found_tasks = []
            current_task = {}
            
            for line in lines:
                line = line.strip()
                if line.startswith('TaskName:'):
                    if current_task and any('rs' in str(current_task.values()).lower() or 'forwarder' in str(current_task.values()).lower()):
                        found_tasks.append(current_task)
                    current_task = {'TaskName': line.split(':', 1)[1].strip()}
                elif ':' in line and current_task:
                    key, value = line.split(':', 1)
                    current_task[key.strip()] = value.strip()
            
            # Check last task
            if current_task and any('rs' in str(current_task.values()).lower() or 'forwarder' in str(current_task.values()).lower()):
                found_tasks.append(current_task)
            
            if found_tasks:
                print("Found scheduled tasks that might be restarting RS Forwarder:")
                for task in found_tasks:
                    print(f"  - {task.get('TaskName', 'Unknown')}")
                    if 'Task To Run' in task:
                        print(f"    Command: {task['Task To Run']}")
            else:
                print("No RS Forwarder related tasks found in Task Scheduler.")
        else:
            print("Could not query Task Scheduler (may need admin rights)")
            print("Run this script as administrator to check scheduled tasks.")
    
    except Exception as e:
        print(f"Error checking Task Scheduler: {e}")
        print("\nTo manually check:")
        print("  1. Open Task Scheduler (taskschd.msc)")
        print("  2. Look for tasks containing 'rs' or 'forwarder'")
        print("  3. Disable or delete any that auto-start the bot")

if __name__ == "__main__":
    check_task_scheduler()


















