#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import string
from pyspark import SparkContext
from config import *

def gen_ngram(terms, n):
	return map(' '.join, zip(*[terms[i:] for i in range(n)]))

# returns a list consisting of n1 to n2 grams
def gen_name_mention_cands(text, n1 = 1, n2 = 8, lower = False):
	terms = filter(None, map(lambda s: s.strip(string.punctuation).lower() if lower else s.strip(string.punctuation), text.replace('\n', ' ').split()))
	ngrams = set(ngram for li in (gen_ngram(terms, n) for n in range(n1, n2 + 1)) for ngram in li)
	return ngrams

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print __file__, 'needs exactly 1 argument!'
		exit(1)
	sc = SparkContext(spark_master, 'name_mention')
	dat = sc.textFile(hdfs_master + sys.argv[1], nsplit)
	cands = dat.flatMap(gen_name_mention_cands).distinct()
	if len(sys.argv) > 2:
		cands.saveAsTextFile(hdfs_master + '/name_mention_cands')
	else:
		print '\n'.join(map(lambda s: s.encode('utf8'), cands.collect()))
	exit(0)
