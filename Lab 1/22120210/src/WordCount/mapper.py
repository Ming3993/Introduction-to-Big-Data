#!/usr/bin/env python3
import sys
import re

# Define a set of specific letters to check against
letters_set = {'a', 'f', 'g', 'j', 'h', 'c', 'm', 'u', 's'}

# Read input line by line from standard input (sys.stdin)
for line in sys.stdin:
    # Split the line into words using non-alphabetic characters as delimiters
    words = re.split(r"[^a-zA-Z]+", line.strip())
    
    # Iterate through each extracted word
    for word in words:
        # Check if the word is not empty and its first letter (in lowercase) is in the predefined set
        if word and word[0].lower() in letters_set:
            # Print the first letter and the count 1, separated by a tab
            print(f"{word[0].lower()}\t1")
