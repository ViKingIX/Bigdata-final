#!/bin/bash

if [ ! $# -eq 1 ]; then
	echo "trans.sh needs exactly 1 argument!"
	exit 1
fi
if [ ! -e $1 ]; then
	echo "$0: can't read $1: No such file"
	exit 1
fi
# 1. remove all newlines
# 2. add newlines after </page>
# 3. add newline before the first <page>
# 4. remove spaces between tags
# 5. remove the first and last line which contain headers
sed ':a;N;$!ba;s/\n//g; s/<\/page>/&\n/g; s/<page>/\n&/; s/>\ *</></g' $1 | sed 's/^\ *</</; 1d; $d;'
# add numbering to line separated by tab
#sed '=' $1 | sed 'N; s/\n/\t'
