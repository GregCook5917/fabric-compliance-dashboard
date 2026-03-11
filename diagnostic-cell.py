# DIAGNOSTIC — remove after troubleshooting
import json

raw = fabric_get(f"workspaces/{CONFIG['dev_workspace_id']}/git/status")

print("Top-level keys:", list(raw.keys()))
print("Total changes:", len(raw.get("changes", [])))
print()
print("Full response (first 2 items):")
print(json.dumps(raw.get("changes", [])[:2], indent=2, default=str))

# DIAGNOSTIC 3 — safely inspect every field in the first change object
change = raw["changes"][0]

print("Change top-level keys:", list(change.keys()))
print()

for key, val in change.items():
    print(f"  {key}: {type(val).__name__} = {repr(val)[:120]}")

    # DIAGNOSTIC — Cell 5 response structure
import json

raw_test = fabric_get(f"workspaces/{CONFIG['test_workspace_id']}/items")

print("Top-level keys:", list(raw_test.keys()))
print()

# Show first item in full
items = raw_test.get("value", [])
print(f"Total items: {len(items)}")
print()
print("First item structure:")
print(json.dumps(items[0], indent=2, default=str))