#!/usr/bin/env python

import sys, yaml, json

print(yaml.dump(json.loads(sys.stdin.read()), indent=2, sort_keys=False))
