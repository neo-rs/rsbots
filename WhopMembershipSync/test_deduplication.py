#!/usr/bin/env python3
"""
Test deduplication logic to see why active members are being removed.
"""

# Simulate what happens during deduplication
test_cases = [
    # Case 1: Active membership with email
    {"email": "test@example.com", "status": "active", "priority": 3},
    # Case 2: Same email, but "left" status (should NOT override active)
    {"email": "test@example.com", "status": "left", "priority": 11},
    # Case 3: Active membership without email, with Discord ID
    {"email": "", "discord_id": "123456789", "status": "active", "priority": 3},
    # Case 4: Same Discord ID, but "left" status
    {"email": "", "discord_id": "123456789", "status": "left", "priority": 11},
    # Case 5: Active membership, no email, no Discord ID (should be skipped)
    {"email": "", "discord_id": "", "status": "active", "priority": 3},
]

print("Testing deduplication logic:")
print("=" * 60)

member_status_map = {}
status_priority = {
    "canceling": 1,
    "renewing": 2,
    "active": 3,
    "trialing": 4,
    "churned": 5,
    "expired": 6,
    "completed": 7,
    "past_due": 8,
    "unresolved": 9,
    "drafted": 10,
    "left": 11,
}

def get_status_priority(status: str) -> int:
    return status_priority.get(status.lower(), 999)

skipped_no_id = 0
processed = 0

for idx, case in enumerate(test_cases, 1):
    email = case.get("email", "")
    discord_id = case.get("discord_id", "")
    status = case["status"]
    
    # Determine member key
    member_key = None
    if email:
        member_key = email.strip().lower()
    elif discord_id:
        member_key = f"discord_{discord_id}"
    
    if not member_key:
        skipped_no_id += 1
        print(f"Case {idx}: SKIPPED - No email or Discord ID")
        continue
    
    processed += 1
    existing = member_status_map.get(member_key)
    current_priority = get_status_priority(status)
    
    if existing:
        existing_priority = get_status_priority(existing.get("status", ""))
        print(f"Case {idx}: Member {member_key} already exists with status '{existing.get('status')}' (priority {existing_priority})")
        print(f"         New status: '{status}' (priority {current_priority})")
        
        # Check the logic
        if existing.get("status", "").lower() == "left" and current_priority < 11:
            print(f"         -> REPLACING 'left' with '{status}' (active membership overrides)")
            member_status_map[member_key] = {"status": status}
        elif current_priority < existing_priority:
            print(f"         -> REPLACING (new status has higher priority)")
            member_status_map[member_key] = {"status": status}
        else:
            print(f"         -> KEEPING existing status")
    else:
        print(f"Case {idx}: NEW member {member_key} with status '{status}'")
        member_status_map[member_key] = {"status": status}

print()
print("=" * 60)
print("RESULTS:")
print(f"Processed: {processed}")
print(f"Skipped (no ID): {skipped_no_id}")
print(f"Final members: {len(member_status_map)}")
print()
print("Final status breakdown:")
for key, data in member_status_map.items():
    print(f"  {key}: {data['status']}")
