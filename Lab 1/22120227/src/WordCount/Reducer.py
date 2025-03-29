import sys

# Initialize a dictionary to store results
letter_count = {}

# Read input line by line (Mapper output)
for line in sys.stdin:
    # Split the data (letter and count)
    try:
        letter, count = line.strip().split("\t")
        count = int(count)
    except ValueError:
        continue # Skip invalid lines
    
    # Accumulate occurrences of each letter
    if letter in letter_count:
        letter_count[letter] += count
    else: 
        letter_count[letter] = count
    
# List of target letters
target_letters = ['a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's']  

# Print final results
for letter in target_letters:
    if letter in letter_count:
        print(f"{letter}\t{letter_count[letter]}")