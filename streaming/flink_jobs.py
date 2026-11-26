"""Print Flink jobs from /jobs/overview JSON on stdin (used by `make stream-status`)."""

import json
import sys

for job in json.load(sys.stdin)["jobs"]:
    print(f"flink job {job['name']}: {job['state']}")
