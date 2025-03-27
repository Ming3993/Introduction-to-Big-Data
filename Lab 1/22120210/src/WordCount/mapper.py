#!/usr/bin/env python 

# import sys because we need to read and write data to STDIN and STDOUT 
import sys 

letters_dict = {'a','f','g','j','h','c','m','u','s'}

def Is_a_word(line):
    for letter in line:
        if not letter.isalpha():
            return False
    return True

# reading entire line from STDIN (standard input) 
for line in sys.stdin: 
	# to remove leading and trailing whitespace 
	line = line.strip() 
	if line and line[0].lower() in letters_dict:
		if not Is_a_word(line):
			continue
		print (f"{line[0].lower()}\t1")
