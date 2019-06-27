#!/usr/bin/env python
import sys
import re
import time
import datetime

# input comes from STDIN (standard input)
for line in sys.stdin:
 # remove leading and trailing whitespace
 line = line.strip()

 # split the line into words
 dates = line.rsplit('[')[1].rsplit(']')[0].rsplit('-')[0][3:11]
 ym = datetime.datetime.strptime(dates,'%b/%Y').date()
 sym = ym.strftime('%Y-%m')
 print("%s\t%s" % (sym,1))

