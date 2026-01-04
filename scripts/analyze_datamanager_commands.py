#!/usr/bin/env python3
"""
Analyze DataManagerBot Commands - Detailed Check
-------------------------------------------------
Checks which commands actually work vs disabled/stubs.
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple

def analyze_command_function(content: str, func_start: int) -> Dict[str, any]:
    """Analyze a command function to determine its status."""
    # Get function body (next 3000 chars or until next def)
    func_end = func_start + 3000
    next_def = content.find('\n        async def ', func_start + 100)
    if next_def != -1 and next_def < func_end:
        func_end = next_def
    
    func_body = content[func_start:func_end]
    
    # Check for disabled/removed markers
    disabled_phrases = [
        'Feature Disabled',
        'has been removed',
        'eBay market analysis has been removed',
        '# DISABLED',
        '# Removed code',
        'return  # disabled'
    ]
    
    is_disabled = any(phrase in func_body for phrase in disabled_phrases)
    
    # Check if it just returns early with error
    early_returns = func_body.count('return\n') + func_body.count('return  #')
    has_early_return_only = early_returns > 2 and len(func_body) < 500
    
    # Check for actual implementation (calls, operations, etc.)
    has_implementation = any(marker in func_body for marker in [
        'await ctx.send',
        'await status_msg.edit',
        'self.',
        'os.',
        'json.',
        'requests.',
        'discord.',
        'load_',
        'save_',
        'get_',
        'set_',
        'create_',
        'delete_',
        'fetch_',
        'restart_'
    ]) and len(func_body) > 300
    
    # Determine status
    if is_disabled:
        status = "DISABLED"
    elif has_early_return_only and not has_implementation:
        status = "STUB"
    elif has_implementation:
        status = "WORKING"
    else:
        status = "UNKNOWN"
    
    return {
        "status": status,
        "is_disabled": is_disabled,
        "has_implementation": has_implementation,
        "body_length": len(func_body),
        "early_returns": early_returns
    }

def scan_all_commands() -> Dict[str, Dict]:
    """Scan all prefix commands in datamanagerbot.py."""
    bot_file = Path(__file__).parent.parent / "neonxt" / "bots" / "datamanagerbot.py"
    
    with open(bot_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    commands = {}
    
    # Find all @self.bot.command decorators
    pattern = r'@self\.bot\.command\(name=[\'"]([^\'"]+)[\'"]'
    
    for match in re.finditer(pattern, content):
        cmd_name = match.group(1)
        pos = match.start()
        line_num = content[:pos].count('\n') + 1
        
        # Find function definition
        func_search_start = pos
        func_match = re.search(r'async def (\w+_command)\(', content[func_search_start:func_search_start+500])
        
        if not func_match:
            continue
        
        func_name = func_match.group(1)
        func_start = content.find(f"async def {func_name}", func_search_start)
        
        if func_start == -1:
            continue
        
        # Get docstring
        doc_match = re.search(r'"""(.*?)"""', content[func_start:func_start+500], re.DOTALL)
        docstring = ""
        if doc_match:
            docstring = doc_match.group(1).strip()
            # Clean up docstring
            docstring = re.sub(r'\s+', ' ', docstring)
            docstring = docstring[:150]
        
        # Analyze function
        analysis = analyze_command_function(content, func_start)
        
        # Check for channel requirement
        func_body = content[func_start:func_start+1000]
        requires_channel = 'COMMAND_CHANNEL_ID' in func_body or '_validate_command_channel' in func_body
        
        commands[cmd_name] = {
            "name": cmd_name,
            "line": line_num,
            "function": func_name,
            "docstring": docstring,
            "requires_channel": requires_channel,
            **analysis
        }
    
    return commands

def main():
    print("=" * 70)
    print("  DATAMANAGERBOT PREFIX COMMANDS - DETAILED ANALYSIS")
    print("=" * 70)
    
    commands = scan_all_commands()
    
    # Group by status
    working = [c for c in commands.values() if c["status"] == "WORKING"]
    disabled = [c for c in commands.values() if c["status"] == "DISABLED"]
    stubs = [c for c in commands.values() if c["status"] == "STUB"]
    
    print(f"\n📊 Summary:")
    print(f"  Total Commands: {len(commands)}")
    print(f"  ✅ Working: {len(working)}")
    print(f"  ❌ Disabled: {len(disabled)}")
    print(f"  ⚠️  Stubs/Incomplete: {len(stubs)}")
    
    if working:
        print(f"\n{'=' * 70}")
        print("  ✅ WORKING COMMANDS")
        print("=" * 70)
        for cmd in sorted(working, key=lambda x: x["name"]):
            print(f"\n  !{cmd['name']}")
            print(f"    Line: {cmd['line']} | Function: {cmd['function']}")
            if cmd['docstring']:
                print(f"    {cmd['docstring']}")
            if cmd['requires_channel']:
                print(f"    🔒 Requires: COMMAND_CHANNEL_ID")
    
    if disabled:
        print(f"\n{'=' * 70}")
        print("  ❌ DISABLED COMMANDS (Should be removed)")
        print("=" * 70)
        for cmd in sorted(disabled, key=lambda x: x["name"]):
            print(f"\n  !{cmd['name']}")
            print(f"    Line: {cmd['line']} | Function: {cmd['function']}")
            if cmd['docstring']:
                print(f"    {cmd['docstring']}")
    
    if stubs:
        print(f"\n{'=' * 70}")
        print("  ⚠️  STUBS/INCOMPLETE COMMANDS")
        print("=" * 70)
        for cmd in sorted(stubs, key=lambda x: x["name"]):
            print(f"\n  !{cmd['name']}")
            print(f"    Line: {cmd['line']} | Function: {cmd['function']}")
            print(f"    Body length: {cmd['body_length']} chars | Early returns: {cmd['early_returns']}")
            if cmd['docstring']:
                print(f"    {cmd['docstring']}")
    
    print(f"\n{'=' * 70}")
    print("  RECOMMENDATIONS")
    print("=" * 70)
    
    if working:
        print(f"\n✅ {len(working)} working command(s) could be migrated to slash commands:")
        for cmd in sorted(working, key=lambda x: x["name"]):
            print(f"   • !{cmd['name']} → /{cmd['name']}")
    
    if disabled:
        print(f"\n🗑️  {len(disabled)} disabled command(s) should be removed:")
        for cmd in sorted(disabled, key=lambda x: x["name"]):
            print(f"   • !{cmd['name']} (line {cmd['line']})")
    
    print()

if __name__ == "__main__":
    main()

