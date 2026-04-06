import json
import random
from pathlib import Path

def simulate():
    print("Simulating platform-wide failures...")
    
    # 1. Injected range violation in week 3
    # Already done in previous steps, but let's ensure it's there
    p3 = Path("outputs/week3/extractions.jsonl")
    if p3.exists():
        lines = p3.read_text().splitlines()
        new_lines = []
        for line in lines:
            data = json.loads(line)
            if "extracted_facts" in data:
                for fact in data["extracted_facts"]:
                    # Inject a high confidence value (85.0 instead of 0.85)
                    fact["confidence"] = 85.0
            new_lines.append(json.dumps(data))
        p3.write_text("\n".join(new_lines))
        print("Injected range violation into Week 3 extractions.")

    # 2. Injected UUID violation in week 2
    p2 = Path("outputs/week2/verdicts.jsonl")
    if p2.exists():
        lines = p2.read_text().splitlines()
        new_lines = []
        for line in lines:
            data = json.loads(line)
            # Corrupt UUID
            data["rubric_id"] = "invalid-uuid-format"
            new_lines.append(json.dumps(data))
        p2.write_text("\n".join(new_lines))
        print("Injected UUID violation into Week 2 verdicts.")

    # 3. Simulate drift by changing content in Week 3
    # This will trigger embedding drift next time ai_extensions.py runs
    print("Drift simulation prepared (data content modified).")

if __name__ == "__main__":
    simulate()
