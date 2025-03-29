import sys
import re

# List of target letters to count
target_letters = ['a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's']

# Read input line by line (from words.txt)
for line in sys.stdin:
    # Split words, also separating words with special characters
    words = re.findall(r'[a-zA-Z]+', line) # Extract only alphabetic sequences

    # Iterate through each word
    for word in words:
        first_letter = word[0].lower() # Get the first letter and convert it to lowercase

        # Check if the first letter is in the target_letters list
        if first_letter in target_letters:
            print(f"{first_letter}\t1")  # Output format: "first letter \t 1" for the Reducer