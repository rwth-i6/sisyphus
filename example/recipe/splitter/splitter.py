#!/usr/bin/env python3
# Simple example script to split a text file into paragraphs
import sys


def main():
    outprefix = sys.argv[1]
    counter = 0
    current_output = None

    for line in sys.stdin:
        if not line.strip():
            if current_output:
                current_output.close()
            current_output = None
        else:
            if not current_output:
                current_output = open("%s%03i" % (outprefix, counter), 'wt')
                counter += 1
            current_output.write(line)


if __name__ == '__main__':
    main()
