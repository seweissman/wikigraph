#!/bin/bash

#By default, the prefix of a line up to the first tab character is the key and the rest of the line (excluding the tab character) will be the value.
etc/hadoop-stream.sh -inputreader "StreamXmlRecordReader,begin=revision,end=revision" -input $1 -output $2 -mapper /bin/cat
