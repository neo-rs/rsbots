#!/usr/bin/env python3
"""
Test DataManagerBot Prefix Commands
-----------------------------------
Checks which commands exist, their status, and if they're functional.
"""

import re
import ast
from pathlib import Path
from typing import Dict, List, Any

def scan_datamanager_commands() -> Dict[str, Any]:
    """Scan datamanagerbot.py for all prefix commands."""
    bot_file = Path(__file__).parent.parent / "neonxt" / "bots" / "datamanagerbot.py"
    
    if not bot_file.exists():
        return {"error": "datamanagerbot.py not found"}
    
    commands = {}
    
    with open(bot_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
        lines = content.split('\n')
    
    # Find all @self.bot.command decorators
    pattern = r'@self\.bot\.command\(name=[\'"]([^\'"]+)[\'"]'
    
    for match in re.finditer(pattern, content):
        cmd_name = match.group(1)
        pos = match.start()
        line_num = content[:pos].count('\n') + 1
        
        # Find the function definition
        func_match = re.search(r'async def (\w+_command)\(', content[pos:pos+500])
        func_name = func_match.group(1) if func_match else "unknown"
        
        # Get function body
        func_start = content.find(f"async def {func_name}", pos)
        if func_start == -1:
            continue
        
        # Find function end (next def or class at same indent)
        func_end = func_start
        indent_level = 0
        for i, char in enumerate(content[func_start:func_start+5000]):
            if char == '\n':
                line = content[func_start+i:func_start+i+200].split('\n')[0]
                if line.strip().startswith('async def ') or line.strip().startswith('def '):
                    if i > 100:  # Not the same function
                        func_end = func_start + i
                        break
        
        func_body = content[func_start:func_end][:2000]
        
        # Check if disabled/removed
        is_disabled = any(phrase in func_body.lower() for phrase in [
            'feature disabled',
            'removed',
            'disabled',
            'not implemented',
            'todo',
            'pass  #',
            'return  # disabled'
        ])
        
        # Check if it has actual implementation
        has_implementation = not any(phrase in func_body for phrase in [
            'pass\n',
            'return\n',
            'Feature Disabled',
            'has been removed'
        ]) and len(func_body) > 200
        
        # Check for channel validation
        requires_channel = 'COMMAND_CHANNEL_ID' in func_body or '_validate_command_channel' in func_body
        
        # Get docstring
        doc_match = re.search(r'"""(.*?)"""', func_body, re.DOTALL)
        docstring = doc_match.group(1).strip() if doc_match else "No description"
        
        commands[cmd_name] = {
            "name": cmd_name,
            "line": line_num,
            "function": func_name,
            "disabled": is_disabled,
            "has_implementation": has_implementation,
            "requires_channel": requires_channel,
            "docstring": docstring[:200],
            "status": "DISABLED" if is_disabled else ("WORKING" if has_implementation else "STUB")
        }
    
    return commands

def print_command_report(commands: Dict[str, Any]):
    """Print formatted command report."""
    print("=" * 70)
    print("  DATAMANAGERBOT PREFIX COMMANDS STATUS")
    print("=" * 70)
    
    total = len(commands)
    working = sum(1 for c in commands.values() if c["status"] == "WORKING")
    disabled = sum(1 for c in commands.values() if c["status"] == "DISABLED")
    stubs = sum(1 for c in commands.values() if c["status"] == "STUB")
    
    print(f"\n📊 Summary:")
    print(f"  Total Commands: {total}")
    print(f"  ✅ Working: {working}")
    print(f"  ❌ Disabled: {disabled}")
    print(f"  ⚠️  Stubs: {stubs}")
    
    print(f"\n{'=' * 70}")
    print("  COMMAND DETAILS")
    print("=" * 70)
    
    for cmd_name, cmd_info in sorted(commands.items()):
        status_icon = {
            "WORKING": "✅",
            "DISABLED": "❌",
            "STUB": "⚠️"
        }.get(cmd_info["status"], "❓")
        
        print(f"\n{status_icon} `!{cmd_name}`")
        print(f"   Status: {cmd_info['status']}")
        print(f"   Line: {cmd_info['line']}")
        print(f"   Function: {cmd_info['function']}")
        
        if cmd_info["requires_channel"]:
            print(f"   🔒 Requires: COMMAND_CHANNEL_ID (1435546857085341857)")
        
        if cmd_info["docstring"]:
            doc_lines = cmd_info["docstring"].split('\n')[:3]
            print(f"   Description: {doc_lines[0]}")
            for line in doc_lines[1:]:
                if line.strip():
                    print(f"                {line.strip()}")
        
        if cmd_info["disabled"]:
            print(f"   ⚠️  This command is DISABLED/REMOVED")
    
    print(f"\n{'=' * 70}")
    print("  RECOMMENDATIONS")
    print("=" * 70)
    
    if disabled > 0:
        print(f"\n❌ {disabled} command(s) are disabled and should be removed:")
        for cmd_name, cmd_info in commands.items():
            if cmd_info["status"] == "DISABLED":
                print(f"   • !{cmd_name} (line {cmd_info['line']})")
    
    if working > 0:
        print(f"\n✅ {working} command(s) are working and could be migrated to slash commands:")
        for cmd_name, cmd_info in commands.items():
            if cmd_info["status"] == "WORKING":
                print(f"   • !{cmd_name} → /{cmd_name}")

def main():
    print("\n🔍 Scanning datamanagerbot.py for prefix commands...\n")
    
    commands = scan_datamanager_commands()
    
    if "error" in commands:
        print(f"❌ Error: {commands['error']}")
        return 1
    
    print_command_report(commands)
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())

