import sys

for line in sys.stdin:
    #remove space from beginning and end of the file
    line = line.strip()

    #split into words
    words = line.split()

    #output tuples
    for word in words:
        print(word, "1")