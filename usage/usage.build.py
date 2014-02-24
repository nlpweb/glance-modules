# -*- coding: utf-8 -*-

## system 
import getopt, sys, json
from pprint import pprint
from collections import defaultdict

## mongo
import pymongo

## pymodules
from ListCombination import ListCombination
import sqlitedb, dependency, color

## nltk
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
	pprint(patterns)

	# mco.find_and_modify(query=query, update=update)

def _extract_opt(argv):
	halt, target, rule, limit, dump = None, None, None, None, None
	try:
		opts, args = getopt.getopt(argv,"pdht:r:l:",["pause","dump","help", "target=", "rule=", "limit="])
	except getopt.GetoptError:
		print >> sys.stderr, 'python usage.build.py [-t <target>] [-r <rules>] [-l <limit>] [-p <pause>]'
		sys.exit(2)
	for opt, arg in opts:
		if opt in ('-h', '--help'): 
			print >> sys.stderr, '[usage]'
			print >> sys.stderr, '\tpython usage.build.py [-t <target>] [-r <rules>] [-l <limit>] [-p <pause>]'
			sys.exit(2)
		elif opt in ('-t', '--target'): target = arg
		elif opt in ('-r', '--rule'): rule = arg
		elif opt in ('-l', '--limit'): limit = arg
		elif opt in ('-d', '--dump'): dump = True
		elif opt in ('-p', '--pause'): halt = True
	return {'target':target, 'rule':rule, 'limit':limit, 'dump':dump, 'halt':halt}

def main(argv, halt=False):

	# default value
	target = 'familiar'
	rule = [('subj', 1), ('cop', 1), ('prep', 1)]
	limit = -1
	dump = False

	var = _extract_opt(argv)
	target = target if not var['target'] else var['target'].strip()
	rule = rule if not var['rule'] else eval(var['rule'])
	limit = limit if not var['limit'] else int(var['limit'])
	dump = dump if not var['dump'] else var['dump']
	halt = halt if not var['halt'] else var['halt']

	print >> sys.stderr, color.render("target:",'lc'),target
	print >> sys.stderr, color.render("rule:",'lc'),rule
	print >> sys.stderr, color.render("limit:",'lc'),limit
	print >> sys.stderr, color.render("dump:",'lc'),dump

	if halt:
		print >> sys.stderr, 'press to begin ...',raw_input()
	

	## ------------------------------ main program ------------------------------

	R = coDeps.find({'lemma': target}) if limit < 0 else coDeps.find({'lemma': target}).limit(limit)
	
	for entry in R:

		# get dependency relations 
		deps = entry['deps']

		# fetch original sentence info (including raw tree) to obtain pos tags
		raw = list(coParsed.find( {'id':entry['sid']} ))[0]

		tree = Tree(raw['tree'])

		# filter deps by pre-defined rule
		# and yield a dictionary with rel<str> as key, deps<list> as value
		rels = apply_rule(deps, rule)

		if not rels: continue

		combs = ListCombination(rels.values())
		
		# calculate weight of each combination
		weight = 1/float(len(combs)) if len(combs) > 1 else 1.0

		# form the anchor element using (word, index pair)
		anchor = (entry['word'], entry['idx'])

		# collect existing patterns object, ready to append new found patterns
		patterns = [] if 'patterns' not in entry else entry['patterns'] 

		# print 'sid >',

		for comb in combs:

			words = form(comb, anchor, tree)
			if not words: continue
			pattern = {'rule': rule, 'words': words, 'weight': weight}

			if pattern not in patterns:
				patterns.append(pattern)

			words_str = ' '.join([ color.render(x[0],'g') for x in words])
			print '(%s) %s' % (entry['sid'], words_str)
		
		## update mongo document
		if dump:
			save_extracted_patterns(mco=coDeps, sid=entry['sid'], lemma=target, patterns=patterns)
			raw_input()

if __name__ == '__main__':
	
	main(sys.argv[1:])


