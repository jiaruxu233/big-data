#!/usr/bin/env python
from operator import itemgetter
import sys
current_date = None
current_count = 0
date = None
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    date, count = line.split('\t', 1)
    int_count = int(count)
    # this IF-switch only works because Hadoop sorts map output by key (here: word) before it is passed to the reducer
    if current_date == date:
        current_count += int_count
    else:
        if current_date:
            # write result to STDOUT
            print("%s\t%s" % (current_date, current_count))
        current_count = int_count
        current_date = date
# do not forget to output the last word if needed!
if current_date == date:
    print("%s\t%s" % (current_date, current_count))

