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
		text = soup.findChild('text').text
		title = soup.title.text
	except Exception:
		return []
	links = parselinks(title, text)
	if links is None:
		return []
	cands_page = map(str.lower, gen_name_mention_cands(text))
	cands = filter(lambda cand: cand in cands_page, cands)
	return map(lambda cand: (cand, (int(cand in links), 1)), cands)

def pykp(dat, cands):
	lines = dat.takeSample(False, 16, random.randint(0, 1024))
	#lines = dat.collect()
	kp = []
	for cand, group in groupby(sum(filter(None, map(lambda line: parselinks_kp(line, cands), lines)), []), itemgetter(0)):
		nkey = 0
		noccur = 0
		for cand, v in group:
			nkey += v[0]
			noccur += v[1]
		kp.append((cand, float(nkey) / noccur if noccur > 0 else 0))
	#print '\n'.join(map(lambda v: '%s\t%f' % (v[0], v[1]), kp))
	print '\n'.join(map(lambda (cand, kp): '%s\t%f' % (cand, kp), filter(lambda (cand, kp): kp > 0, kp)))

	kpstep = 0.01
	hist = []
	for k, group in groupby(map(lambda (cand, kp): (math.floor(kp / kpstep) * kpstep, 1), kp), itemgetter(0)):
		count = reduce(lambda v1, v2: (v1[0], v1[1] + v2[1]), group)
		hist.append((k, count))
	print '\n'.join(map(lambda v: '%s\t%d' % (v[0], v[1]), hist))
	return

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print >> sys.stderr, __file__, 'needs 1 argument!'
		exit(1)
	sc = SparkContext(spark_master, __file__, pyFiles=['parsewiki.py', 'name_mention.py'])
	dat = sc.textFile(hdfs_master + sys.argv[1], nsplit)
	name_mention_cands = sc.textFile(hdfs_master + '/name_mention_cands').map(str.lower).collect()
	candsb = sc.broadcast(name_mention_cands)
	
	kp = dat.flatMap(lambda line: parselinks_kp(line, candsb.value)) \
		.filter(None).reduceByKey(lambda (nkey1, noccur1), (nkey2, noccur2): (nkey1 + nkey2, noccur1 + noccur2)) \
		.mapValues(lambda (nkey, noccur): float(nkey) / noccur if noccur > 0 else 0) \
		.filter(lambda kp: kp > 0)
	kp.map(lambda (cand, kp): '%s\t%f' % (cand, kp)).saveAsTextFile(hdfs_master + '/keyphraseness')

	kpstep = 0.01
	hist = kp.map(lambda (cand, kp): (math.floor(kp / kpstep) * kpstep, 1))\
		 .reduceByKey(operator.add)
	hist.map(lambda (kp, count): '%f\t%d' % (kp, count)).saveAsTextFile(hdfs_master + '/keyphraseness_hist')
	exit(0)
