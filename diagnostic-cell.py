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

# DIAGNOSTIC — Cell 6 raw response
import json

raw_ops = fabric_get(
    f"deploymentPipelines/{CONFIG['deployment_pipeline_id']}/operations"
)

print("Top-level keys:", list(raw_ops.keys()))
print("Total operations:", len(raw_ops.get("value", [])))
print()

# Show first operation in full if any exist
ops = raw_ops.get("value", [])
if ops:
    print("First operation structure:")
    print(json.dumps(ops[0], indent=2, default=str))
else:
    print("No operations returned — checking alternate keys...")
    for k, v in raw_ops.items():
        print(f"  {k}: {type(v).__name__} = {repr(v)[:200]}")