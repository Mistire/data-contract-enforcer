import json
from pathlib import Path

def inject_violation():
    data_path = Path("outputs/week3/extractions.jsonl")
    if not data_path.exists():
        print(f"Error: {data_path} not found")
        return

    with open(data_path, "r") as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        try:
            record = json.loads(line)
            # Inject violation: set confidence to 85.0 (range is 0.0-1.0)
            if "extracted_facts" in record:
                for fact in record["extracted_facts"]:
                    fact["confidence"] = 85.0
            new_lines.append(json.dumps(record))
        except Exception as e:
            print(f"Error parsing line: {e}")

    violation_path = Path("outputs/week3/extractions_violated.jsonl")
    with open(violation_path, "w") as f:
        for line in new_lines:
            f.write(line + "\n")
    print(f"✓ Injected violation into {violation_path}")

if __name__ == "__main__":
    inject_violation()
