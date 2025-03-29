import sys

key_set = {'a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's'}  

for line in sys.stdin:
    line = line.strip()
    words = [''.join(filter(str.isalpha, word)) for word in line.split() if any(c.isalpha() for c in word)]

    for word in words:
        if word[0].lower() in key_set:
            print(f"{word[0].lower()}\t{word}")