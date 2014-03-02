# -*- coding: utf-8 -*-
import pymongo
import color
import pickle, os, sys, re
from collections import defaultdict, Counter

doraemon = 'doraemon.iis.sinica.edu.tw'
lost = 'lost.nlpweb.org'

mc = pymongo.Connection(lost)
co = mc['BNC']['usages']

def fetch(co, query): return list(co.find({'anchor': query}))

def aggregate(usages):
	aU = Counter()
	for doc in usages:
		ustr = re.sub(r'#[^\s]+',r'\g<0>'+'.'+doc['pos'].lower()[0], doc['usage'])
		aU[ustr] += float(doc['weight'])
	return aU


def replacing(U):

	posU = {}

	# newU = Counter()
	for usage in U:

		anchor = re.findall(r'#[^\s]+', usage)
		if not anchor: continue
		else: anchor = anchor[0]

		if anchor not in posU:
			posU[anchor] = Counter()

		modify = False
		new_usage = usage
		for supersense in re.findall(r'[A-Z]+',new_usage):

			marker = 'SOMETHING'
			if supersense in ('PERSON', 'GROUP'): 
				marker = 'PERSON'
				# print usage

			if supersense == 'ACT': marker = 'DOING_SOMETHING'
			new_usage = new_usage.replace(supersense, marker)

			modify = True
			# print 'supersense:',supersense, '-->','marker',marker

		if not modify:
			## skip cases such as "#deal with", "#deal with that", those cannot represent using super senses
			continue

		# if new_usage not in U:
			# print 'old:',usage,'-->','new:',new_usage

		posU[anchor][new_usage] += float(U[usage])

	return posU

## a usage will be counted if it occurs more than <min_cnt=5> counts
def filtering(posU, min_cnt=1):
	posP = {}
	for anchor in posU:
		P = Counter()
		S = 0
		skip = set()
		for usage in posU[anchor]:
			if posU[anchor][usage] < min_cnt:
				skip.add(usage)
				continue
			S += posU[anchor][usage]
			
		S = float(S)
		if S == 0.0: continue
		for usage in posU[anchor]:
			if usage in skip:
				continue
			P[usage] = posU[anchor][usage]/S
		posP[anchor] = P
	return posP

def show_most_common(posU, posP, threshold=1.0, max_usage_cnt=10, min_cnt=10 ,min_percent=0.01):
	for anchor in posP:
		print '='*10,anchor,'='*10

		itemP = sorted(posP[anchor].items(), key=lambda x:x[1], reverse=True)
		accum = 0.0
		for (i, (usage, portion)) in enumerate(itemP):
			# if : break
			if i == max_usage_cnt: break
			if portion < min_percent and posU[anchor][usage] < min_cnt: break
			accum += portion

			colorful = []
			for x in usage.split():
				if x == 'PERSON':
					colorful.append(color.render('PERSON', 'g'))
				elif x == 'SOMETHING':
					colorful.append(color.render('SOMETHING', 'r'))
				elif re.match(r'#[^\.]+\.[a-z]', x): # "#familiar.j"
					colorful.append(color.render(x, 'lc'))
				else:
					colorful.append(x)
			colorful_usage = ' '.join(colorful)

			print colorful_usage,'\t', posU[anchor][usage],'\t', round(posP[anchor][usage]*100.0, 4), '%'
			if accum >= threshold:
				break

def show(collected):
	for anchor in collected:
		print '='*10, anchor, '='*10
		for (i, (usage, usage_cnt, usage_portion)) in enumerate(collected[anchor]):
			colorful = []
			for x in usage.split():
				if x == 'PERSON':
					colorful.append(color.render('PERSON', 'g'))
				elif x == 'SOMETHING':
					colorful.append(color.render('SOMETHING', 'r'))
				elif re.match(r'#[^\.]+\.[a-z]', x): # "#familiar.j"
					colorful.append(color.render(x, 'lc'))
				else:
					colorful.append(x)
			colorful_usage = ' '.join(colorful)

			print colorful_usage,'\t', usage_cnt,'\t', round(usage_portion*100.0, 4), '%'


def filter_most_common(posU, posP, threshold=1.0, max_usage_cnt=10, min_cnt=10 ,min_percent=0.01):
	C = {}
	for anchor in posP:
		itemP = sorted(posP[anchor].items(), key=lambda x:x[1], reverse=True)
		accum = 0.0
		collect = []
		for (i, (usage, portion)) in enumerate(itemP):
			usage_cnt = posU[anchor][usage]
			if i == max_usage_cnt: break
			if portion < min_percent and usage_cnt < min_cnt: break
			accum += portion
			collect.append((usage, usage_cnt, portion))
			if accum >= threshold:
				break
		if collect:
			C[anchor] = collect
	return C

def dumping(query, root, data):
	path = os.path.join(root, query+'.pkl')
	try:
		pickle.dump(data, open(path, 'w'))
		print 'pickle',query,'to',path
	except:
		print 'dumpping error to path',path

if __name__ == '__main__':

	query = 'familiar'
	pickle_root = 'pickle'
	cache_root = 'cache'

	if len(sys.argv) > 1:
		query = sys.argv[1]

	# queries = ['yell', 'agree', 'hear', 'glance', 'separate', 'impress', 'consist', 'listen', 'apply', 'ask', 'aim', 'seek', 'look', 'account', 'launch', 'create', 'build', 'construct', 'contend', 'replace', 'substitute', 'count', 'deal', 'hide', 'provide']

	cache_path = os.path.join(pickle_root,query+'.pkl')

	if not os.path.exists(cache_path):
		data = fetch(co, query)
		dumping(query, root, data)

	else:
		data = pickle.load(open(cache_path, 'r'))

		U = aggregate(data)

		posU = replacing(U)
		posP = filtering(posU, min_cnt=1)

		collected = filter_most_common(posU, posP, threshold=1.0, max_usage_cnt=10, min_cnt=10 ,min_percent=0.01)

		show(collected)
		# show_most_common(posU, posP)
