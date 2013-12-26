#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import math
from itertools import groupby
import operator
from operator import itemgetter
import string
from bs4 import BeautifulSoup as bs
from pyspark import SparkContext
from parsewiki import parselinks
from config import *

def parselinks_kp(page, cands):
	try:
		soup = bs(page)
		title = soup.title.text
		text = soup.findChild('text').text
	except Exception:
		return []
	links = parselinks(title, text, True)
	# returns an empty list if redirected
	if links is None:
		return []
	cands_page = ' '.join(map(lambda s: s.strip(string.punctuation).lower(), text.split()))
	cands = filter(lambda cand: cand in cands_page, cands)
	return map(lambda cand: (cand, (int(cand in links), 1)), cands)

if __name__ == '__main__':
	argc = len(sys.argv)
	if argc < 2:
		print >> sys.stderr, __file__, 'needs at least 1 argument!'
		exit(1)
	sc = SparkContext(spark_master, __file__, pyFiles=['parsewiki.py', 'name_mention.py', 'config.py'])
	name_mention_cands = sc.textFile(hdfs_master + '/name_mention_cands').map(lambda line: line.lower()).collect()
	candsb = sc.broadcast(name_mention_cands)
	
	dat = sc.textFile(hdfs_master + sys.argv[1], nsplit)
	kp = dat.flatMap(lambda line: parselinks_kp(line, candsb.value)) \
		.filter(None) \
		.reduceByKey(lambda (nkey1, noccur1), (nkey2, noccur2): (nkey1 + nkey2, noccur1 + noccur2)) \
		.mapValues(lambda (nkey, noccur): float(nkey) / noccur if noccur > 0 else 0) \
		.filter(lambda (cand, kp): kp > 0)
	if argc > 2:
		kp.map(lambda (cand, kp): '%s\t%f' % (cand, kp)) \
		  .saveAsTextFile(hdfs_master + sys.argv[2])
	else:
		for cand, kp in kp.collect():
			print '%s\t%f' % (cand, kp)

	kpstep = 0.0001
	hist = kp.map(lambda (cand, kp): (math.floor(kp / kpstep) * kpstep, 1)) \
		 .reduceByKey(operator.add)
	if argc > 3:
		hist.map(lambda (kp, count): '%f\t%d' % (kp, count)) \
		    .saveAsTextFile(hdfs_master + sys.argv[3])
	else:
		for kp, count in hist.collect():
			print '%f\t%d' % (kp, count)
	exit(0)
