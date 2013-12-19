#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import math
from itertools import groupby
import operator
from operator import itemgetter
from bs4 import BeautifulSoup as bs
from pyspark import SparkContext
from parsewiki import parselinks
from name_mention import gen_name_mention_cands
from config import *

def parselinks_kp(page, cands):
	try:
		soup = bs(page)
		title = soup.title.text
		text = soup.findChild('text').text
	except Exception:
		return []
	links = parselinks(title, text)
	if links is None:
		return []
	# True for lowercased ngrams
	cands_page = gen_name_mention_cands(text, True)
	cands = filter(lambda cand: cand in cands_page, cands)
	return map(lambda cand: (cand, (int(cand in links), 1)), cands)

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print >> sys.stderr, __file__, 'needs 1 argument!'
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
	kp.map(lambda (cand, kp): '%s\t%f' % (cand, kp)) \
	  .saveAsTextFile(hdfs_master + '/keyphraseness')

	kpstep = 0.0001
	hist = kp.map(lambda (cand, kp): (math.floor(kp / kpstep) * kpstep, 1)) \
		 .reduceByKey(operator.add)
	hist.map(lambda (kp, count): '%f\t%d' % (kp, count)) \
	    .saveAsTextFile(hdfs_master + '/keyphraseness_hist')
	exit(0)
