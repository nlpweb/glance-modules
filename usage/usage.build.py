# -*- coding: utf-8 -*-

import sqlitedb, dependency, color, sys # pymodules require
# from nltk import Tree
from collections import defaultdict
from pprint import pprint
import pymongo
from ListCombination import ListCombination
# from nltk.stem.wordnet import WordNetLemmatizer
# from nltk.corpus import wordnet

######## connect to mongo server ########
doraemon = 'doraemon.iis.sinica.edu.tw'
db_info = {'name': 'BNC', 'collection': 'Deps'}
mc = pymongo.Connection(doraemon)

dbBNC = mc[db_info['name']]
coDeps = dbBNC['Deps']
coParsed = dbBNC['Parsed']

def apply_rule(deps, rule):
	D = defaultdict(list)
	for dep in deps:
		## rel: subj, 1
		for (rel,mincnt) in rule:
			if rel in dep['rel']:
				if rel == 'prep' and '_' in dep['rel']: ## prepc_without, prep_with --> key: prepc_without, prep_with
					D[dep['rel']].append( dep )
				else:
					D[rel].append( dep ) ## xsubj, dobj --> key: subj, dobj
	# pprint(dict(D))
	## check if achieve minimum count
	for (rel,mincnt) in rule:
		if len([x for x in D.keys() if rel in x]) < mincnt:
			return False
	return dict(D)

def form(deps):
	words = set()
	for dep in deps:
		words.add((dep['ltoken'], dep['lidx'], True))
		words.add((dep['rtoken'], dep['ridx'], True))
		## current --> (v1.0) extract prep and predict prep idx
		# (v2.0) get precise prep idx, look into the origin sentence
		if 'prep' in dep['rel']:
			prep = '_'.join(dep['rel'].split('_')[1:])
			idx = max(dep['lidx'], dep['ridx'])
			words.add((prep, idx-1, False))

	words = sorted(list(words), key=lambda x:x[1])
	return words

def findpos(sid, words):
	from nltk import Tree
	res = list(coParsed.find( {'id':sid} ))[0]
	T = Tree(res['tree'])
	T.pos()

if __name__ == '__main__':

	# mongo entry
	# {
	# 	"_id" : ObjectId("53038cd9d4388c4b931e46d3"),
	# 	"word" : "is",
	# 	"idx" : 2,
	# 	"pos" : "VBZ",
	# 	"lemma" : "be",
	# 	"deps" : [
	# 		{
	# 			"ridx" : 2,
	# 			"rtoken" : "is",
	# 			"ltoken" : "innit",
	# 			"rel" : "cop",
	# 			"lidx" : 3
	# 		}
	# 	],
	# 	"sid" : 1
	# }
	target = 'interest'

	R = list(coDeps.find({'lemma': target}).limit(100))
	# R = list(res)

	# rule = [('subj', 1), ('cop', 1), ('prep', 1)]
	rule = [ ('obj', 1), ('prep_in', 1) ]

	for entry in R:

		deps = entry['deps']
		
		rels = apply_rule(deps, rule)  # < dict(<list>) >

		if not rels: continue

		combs = ListCombination(rels.values())
		
		portion = '1/'+str(len(combs)) if len(combs) > 1 else '1'

		print 'sid >',entry['sid']
		for comb in combs:
			words = form(comb)

			words_str = ' '.join([ color.render(x[0],'g') for x in words])

			print portion,'\t',words_str, '\t',words
		print



	# # connect to mongo server
	# print >> sys.stderr, color.render('fetching data','r'), '...',
	# sys.stderr.flush()
	# cur = fetch_mongo(doraemon, db_info, None)
	# print >> sys.stderr, color.render('done','g')

