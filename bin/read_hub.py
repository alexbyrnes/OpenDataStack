import sys
import json
import pprint

data = ''

for line in sys.stdin:
    data += line

datadict = json.loads(data)

for d in datadict:
    print(d)
