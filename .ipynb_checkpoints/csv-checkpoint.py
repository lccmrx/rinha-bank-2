import os
import re

# Your regex pattern
pattern = r"api(\d)\-1.*\|.*\[(\d).*\| (.*) \|.*(\d)\] (.*)\: (.*)"

# Compiling the regex pattern
compiled_pattern = re.compile(pattern)

with open('server.logs', 'rb') as f:
    for line in f:
        line = str(f.readline())

        # Searching for the pattern in the string
        match = compiled_pattern.search(line)

        # Checking if a match was found and printing captured groups
        if match:
            captured_groups = match.groups()
            print("Captured Groups:", captured_groups)
        else:
            print("No match found.")
    os.exit(0)
