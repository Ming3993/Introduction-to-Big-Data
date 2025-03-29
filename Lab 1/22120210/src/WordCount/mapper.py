#!/usr/bin/env python3
import sys
import re

letters_set = {'a', 'f', 'g', 'j', 'h', 'c', 'm', 'u', 's'}

for line in sys.stdin:
    words = re.split(r"[^a-zA-Z]+", line.strip())
    for word in words:
        if word and word[0].lower() in letters_set:
            print(f"{word[0].lower()}\t1")
