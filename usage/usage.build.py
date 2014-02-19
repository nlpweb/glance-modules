# -*- coding: utf-8 -*-

import sqlitedb, dependency, color, sys # pymodules require
# from nltk import Tree
from collections import defaultdict
from pprint import pprint
import pymongo
from ListCombination import ListCombination

from nltk import Tree
# from nltk.stem.wordnet import WordNetLemmatizer
# from nltk.corpus import wordnet

######## connect to mongo server ########
doraemon = 'doraemon.iis.sinica.edu.tw'
db_info = {'name': 'BNC', 'collection': 'Deps'}
mc = pymongo.Connection(doraemon)

dbBNC = mc[db_info['name']]
coDeps = dbBNC['Deps']
coParsed = dbBNC['Parsed']
# coUsage = dbBNC['Parsed']

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
	## check if achieve minimum count
	for (rel,mincnt) in rule:
		if len([x for x in D.keys() if rel in x]) < mincnt:
			return False
	return dict(D)

def form(deps, anchor, tree=None):
	words = set()

	tree_pos = None if not tree else tree.pos()

	## the original dep idx starts from 1
	for dep in deps:
		words.add((dep['ltoken'], dep['lidx'])) # token, idx
		words.add((dep['rtoken'], dep['ridx']))
		## (v1.0) extract prep and predict prep idx
		## current --> (v2.0) get precise prep idx, look into the origin sentence
		if 'prep' in dep['rel']:
			prep = '_'.join(dep['rel'].split('_')[1:])
			if not prep:
				continue

			if not tree:
				idx = max(dep['lidx'], dep['ridx']) - 1
				words.add((prep, idx, False))
			else:
				search = list(enumerate(tree_pos))[ min(dep['lidx'], dep['ridx']) : max(dep['lidx'], dep['ridx'])-1 ]
				## find precise position of prep, also considering multiple preps such as "out of"
				positions = []
				for p in prep.split('_'): ## deal with "out of" cases
					maybe_idxs = [ i for (i,(word, pos)) in search if word == p]
					if not maybe_idxs:  # prep is not like "A prep B". Might caused by parsing error
						continue

					maybe_idx = max(maybe_idxs) + 1
					positions.append(maybe_idx)

				if not positions: # cannot obtain correct position, skip this sentence
					return []

				idx = max(positions)

			words.add((prep, idx))

	# if a tree is given, zip (word, idx, precise) pairs with pos tags
	words = words if not tree else [(word, idx, tree_pos[idx-1][1]) for (word, idx) in words]
	words = sorted(list(words), key=lambda x:x[1])

	# mark anchor node
	words = [(word, idx, pos, (word, idx) == anchor) for (word,idx,pos) in words]

	return words

def save_extracted_patterns(mco, sid, lemma, patterns):

	query = {'sid':sid, 'lemma':lemma}
	update = {'$set': {'patterns':patterns} }

	mco.find_and_modify(query=query, update=update)


if __name__ == '__main__':

	target = 'familiar'

	R = list(coDeps.find({'lemma': target}))
	# R = list(res)

	rule = [('subj', 1), ('cop', 1), ('prep', 1)]
	# rule = [('prep', 1)]
	# rule = [('subj', 1), ('prep', 1), ('dobj', 1)]
	# rule = [ ('obj', 1), ('prep_in', 1) ]

	for entry in R:

		deps = entry['deps']

		raw = list(coParsed.find( {'id':entry['sid']} ))[0]

		tree = Tree(raw['tree'])

		rels = apply_rule(deps, rule)  # < dict(<list>) >

		if not rels: continue

		combs = ListCombination(rels.values())
		
		# portion = '1/'+str(len(combs)) if len(combs) > 1 else '1'

		print 'sid >',entry['sid']

		anchor = (entry['word'], entry['idx'])

		# collect patterns object
		patterns = [] if 'patterns' not in entry else entry['patterns'] 

		for comb in combs:
			words = form(comb, anchor, tree)
			patterns.append({'rule': rule, 'words': words})

			words_str = ' '.join([ color.render(x[0],'g') for x in words])
			print words_str, '\t',words, rule
		print 
		# save_extracted_patterns(mco=coDeps, sid=entry['sid'], lemma=target, patterns=patterns)




