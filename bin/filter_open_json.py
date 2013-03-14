import sys
import json
import pprint

data = ''

for line in sys.stdin:
    data += line

datadict = json.loads(data)

if (len(sys.argv) > 1 and sys.argv[1] == "--socrata"):
    metadata = datadict['meta']['view']
else:
    try:
        metadata = datadict['result']
    except KeyError, TypeError:
        metadata = datadict


print(json.dumps(metadata))
