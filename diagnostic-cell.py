# DIAGNOSTIC — remove after troubleshooting
import json

raw = fabric_get(f"workspaces/{CONFIG['dev_workspace_id']}/git/status")

print("Top-level keys:", list(raw.keys()))
print("Total changes:", len(raw.get("changes", [])))
print()
print("Full response (first 2 items):")
print(json.dumps(raw.get("changes", [])[:2], indent=2, default=str))
