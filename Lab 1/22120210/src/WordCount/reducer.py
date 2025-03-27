import sys
from collections import OrderedDict

# Define desired order
desired_order = ['a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's']
result = OrderedDict((key, 0) for key in desired_order)

# Read input from mapper
for line in sys.stdin:
    letter, cnt = line.split('\t', 1)
    try:
        cnt = int(cnt)
    except ValueError:
        continue

    if letter in result:
        result[letter] += cnt

# Print the result as desired order
for letter in desired_order:
    if result[letter] > 0:
        print(f"{letter}\t{result[letter]}")
