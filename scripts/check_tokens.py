#!/usr/bin/env python3
"""Check which Discord bot each token belongs to."""

import requests
from pathlib import Path

def main():
    env = {}
    env_path = Path(__file__).parent.parent / "config" / "tokenkeys.env"
    
    with open(env_path, 'r') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
    
    print("=" * 60)
    print("  TOKEN -> BOT MAPPING")
    print("=" * 60)
    
    tokens_to_check = [
        'TESTCENTER_BOT_TOKEN',
        'PING_BOT', 
        'DATAMANAGER_BOT'
    ]
    
    for token_name in tokens_to_check:
        token = env.get(token_name, '')
        if not token:
            print(f"{token_name}: NOT SET")
            continue
        
        try:
            headers = {'Authorization': f'Bot {token}'}
            r = requests.get('https://discord.com/api/v10/users/@me', headers=headers, timeout=10)
            
            if r.status_code == 200:
                data = r.json()
                username = data.get('username', '?')
                app_id = data.get('id', '?')
                print(f"{token_name}:")
                print(f"  Bot Name: {username}")
                print(f"  App ID: {app_id}")
            else:
                print(f"{token_name}: Auth failed (HTTP {r.status_code})")
        except Exception as e:
            print(f"{token_name}: Error - {e}")
    
    print("\n" + "=" * 60)
    print("  Which token should register commands?")
    print("=" * 60)
    print("  Commands should be registered using the bot that will")
    print("  RECEIVE the slash command interactions in MirrorWorld.")
    print("  Currently using: TESTCENTER_BOT_TOKEN")

if __name__ == "__main__":
    main()

