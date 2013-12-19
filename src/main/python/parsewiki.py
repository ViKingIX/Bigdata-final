#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import re
import operator
from bs4 import BeautifulSoup as bs
from pyspark import SparkContext
import starbase
from config import *

# text is the text section of a wiki page
# returns a list of links' title
def parselinks(title, text):
	# check redirected page
	res = re.match(r'#REDIRECT \[\[(.*?)(?:#.*?)?\]\]', text)
	if res:
		print >> sys.stderr, title.encode('utf8'), 'is redirected to', res.group(1).encode('utf8')
		return None
	
	# find all links
	links = set(re.findall(r'\[\[([^\]].*?)\]\]', text))

	# filter out links to other language, file, media links
	taglist = ['File', 'media', 'Image', 'User talk']
	links = filter(lambda link: not ':' in link or not (link.split(':')[0].islower() or link.split(':')[0] in taglist or link.split(':')[1] in taglist), links)

	# process renamed links
	# ex: [[link|]], [[link|link_name]]
	links = map(lambda link: link.split('|')[0] if '|' in link else link, links)

	# process link to section
	# ex: [[link#section]], [[#section]]
	links = map(lambda link: title if len(link) > 0 and link[0] == '#' else link.split('#')[0] if '#' in link else link, links)
	
	return links

def parsewiki(line):
	"""
	parses all links that is considered useful from text section in xml string `line`
	"""
	soup = bs(line)
	try:
		pid = int(soup.id.text)
		title = soup.title.text
		text = soup.findChild('text').text
	except Exception:
		print >> sys.stderr, 'bs error on', re.search(r'<title>(.*?)</title>', line).group(1).encode('utf8')
	# skip if the page is a user page, user talk page, etc
	taglist = ['File', 'media', 'Image', 'User talk']
	if ':' in title and title.split(':')[0] in taglist:
		return []
	links = parselinks(title, text)
	if links is None:	# skip redirected page
		return []
	return map(lambda link: (link, pid), links)

if __name__ == '__main__':
	argc = len(sys.argv)
	if argc < 2:
		print >> sys.stderr, __file__, 'needs at least 1 argument!'
		exit(1)
	conn = starbase.Connection(master, starbase_port)
	sc = SparkContext(spark_master, 'parselinks', pyFiles=['config.py'])
	dat = sc.textFile(hdfs_master + sys.argv[1], nsplit)
	radj_list = dat.flatMap(parsewiki) \
		       .filter(None) \
		       .groupByKey()
	if argc > 2:
		radj_list.saveAsTextFile(hdfs_master + sys.argv[2])
	else:
		for title, pids in radj_list.collect()[:5]:
			print title.encode('utf8'), pids
	exit(0)
