#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import re
import operator
from bs4 import BeautifulSoup as bs
from pyspark import SparkContext
import starbase
from config import *

taglist = ['File', 'media', 'Image', 'User_Talk', 'User talk', 'User', 'Talk']

# text is the text section of a wiki page
# returns a list of links' title
def parselinks(title, text, renamed = False):
	# check redirected page
	res = re.match(r'#REDIRECT \[\[(.*?)(?:#.*?)?\]\]', text)
	if res:
		print >> sys.stderr, title.encode('utf8'), 'is redirected to', res.group(1).encode('utf8')
		return None
	
	# find all links
	links = set(re.findall(r'\[\[([^\]].*?)\]\]', text))

	# filter out links to other language, file, media links
	links = filter(lambda link: not (':' in link and (any(field in taglist for field in link.split(':')[:2]) or re.match(r'[a-z]{2,3}:', link))), links)

	# process renamed links
	# ex: [[link|]], [[link|link_name]]
	def process_renamed(link, renamed = False):
		if not '|' in link:
			return link
		fields = link.split('|')
		real = fields[0]
		display = ''
		if len(fields) > 1:
			display = fields[1]
		if not display and renamed:	# auto renamed link
			for delim in ['(', ',']:
				if delim in display:
					display = display.split(delim)[0].rstrip()
			if ':' in display:
				display = display.split(':')[1].lstrip()
		return display if renamed else real
	links = map(lambda link: process_renamed(link, renamed), links)

	# process link to section
	# ex: [[link#section]], [[#section]]
	links = map(lambda link: title if link and link[0] == '#' else link.split('#')[0] if '#' in link else link, links)
	
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
	# skip if the page is a user page, media page, etc
	if ':' in title and not any(field in taglist for field in title.split(':')[:2]):
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
		       .groupByKey(nsplit)
	if argc > 2:
		radj_list.map(lambda (title, pids): '%s\t%s' % (title, ' '.join(map(str, pids)))).saveAsTextFile(hdfs_master + sys.argv[2])
	else:
		for title, pids in radj_list.collect():
			print '%s\t%s' % (title.encode('utf8'), pids)
	exit(0)
