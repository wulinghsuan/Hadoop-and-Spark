`yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar pi 10 100`

In order to make the HDFS organized, create a new folder in HDFS first by using comment `mkdir mapreducer`

Get into local address and copy below two files to HDFS folder (created in Step).

`scp “reducer.py” l.wu-dsti@edge-1.au.adaltas.cloud:/home/l.wu-dsti/mapreducer`

`scp “mapper.py” l.wu-dsti@edge-1.au.adaltas.cloud:/home/l.wu-dsti/mapreducer`

> Or you can just simplely edit two python files in your HDFS by `vi NAME.py`. 
> Example: Create a new python, named **mapper2.py**, by comment `vi mapper2.py`. You will find you are in the editor page now. 
> Copy the following code in it. 

![]()

* `Shit`+`Z`+`Z` to save and exit 
* `Ctrl`+`Z` to shop

*<reducer2.py>*

```
#!/usr/bin/env python
"""reduce.py"""

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print '%s\t%s' % (current_word, current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print '%s\t%s' % (current_word, current_count)
```

*<mapper2.py>*

```
#!/usr/bin/env python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print '%s\t%s' % (word, 1)
```




Back to HDFS

```
yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    -file mapper.py -mapper "python mapper.py" \   
    -file reducer.py -reducer "python reducer.py" \
    -input raw/input.txt \ 
    -output mr/output
```
