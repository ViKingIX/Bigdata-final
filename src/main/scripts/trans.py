#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys

def extract_tag(fn, stag, etag):
	buf = ''
	for line in open(fn, 'r'):
		line = line.rstrip()
		ind1 = line.find(stag)
		ind2 = line.find(etag)
		if ind1 >= 0:
			buf = line[ind1:]
		elif ind2 >= 0:
			res = buf + line[:ind2 + len(etag)]
			buf = ''
			yield res
		elif buf:
			buf += line

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print 'trans.py needs exactly 1 argument!'
		exit(1)
	for page in extract_tag(sys.argv[1], '<page>', '</page>'):
		print page
	exit(0)
