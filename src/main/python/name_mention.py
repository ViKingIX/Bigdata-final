#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import string

def gen_ngram(terms, n):
	return map(' '.join, zip(*[terms[i:] for i in range(n)]))

# returns a list consisting of n1 to n2 grams
def gen_name_mention_cands(text, n1 = 1, n2 = 8):
	terms = filter(None, map(lambda s: s.strip(string.punctuation), text.replace('\n', ' ').split()))
	ngrams = set(ngram for li in (gen_ngram(terms, n) for n in range(n1, n2 + 1)) for ngram in li)
	return ngrams

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print __file__, 'needs exactly 1 argument!'
		exit(1)
	ngrams = set()
	for fn in os.listdir(sys.argv[1]):
		if fn[0] == '.':
			continue
		text = open(fn).read()
		ngrams = ngrams.union(gen_name_mention_cands(text, 1, 8))
	print '\n'.join(ngrams)
	exit(0)
