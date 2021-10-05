#!/usr/bin/env python

import sys, yaml, json

print(json.dumps(yaml.safe_load(sys.stdin.read()), indent=4))
