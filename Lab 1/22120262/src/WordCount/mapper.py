import sys

key_set = {'a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's'}  

for line in sys.stdin:
    line = line.strip()
    if line[0].lower() in key_set and line.isalpha():
        print(f"{line[0].lower()}\t{line}")