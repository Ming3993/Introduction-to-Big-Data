import sys

current_key = None
count = 0
output = []
key_list = ['a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's']

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, item = line.split('\t')

    if key != current_key:
        if current_key is not None:
            output.append(f"{current_key}\t{count}\n")
        current_key = key
        count = 0
    
    count+=1

if current_key is not None:
    output.append(f"{current_key}\t{count}\n")

key_dict = {char: idx for idx, char in enumerate(key_list)}
def sortKey(string):
    return key_dict.get(string[0], float('inf'))

output.sort(key=sortKey)
for ln in output:
    print(ln)