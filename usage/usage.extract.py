# -*- coding: utf-8 -*-

import sqlitedb, dependency, color, sys # pymodules require
from nltk import Tree
from collections import defaultdict
from pprint import pprint
import pymongo
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import wordnet

### fetch_sqlite function
### get results from a sqlite3 database
### e.g., 
###		sql = "select * from BNC_Parsed where sent like ?"
### 	args = ['%'+'interested'+'%']
### return <list> rows of select results
def fetch_sqlite(db_path, sql, args=()):
	# connect to database, fetch data
	(con, cur) = sqlitedb.connect(db_path)
	if '?' in sql:
		res = cur.execute(sql, args).fetchall()	# id int, sent text, POS text, Tree text, Dep text
	else:
		res = cur.execute(sql).fetchall()	# id int, sent text, POS text, Tree text, Dep text
	rows = list(res)
	return rows

### fetch_mongo function
### get results from a mongo database
### e.g,
###		server_addr = "lost.nlpweb.org"
### 	db_info = {'name': 'BNC', 'collection': 'Parsed'}
### return <Cursor> mongo cursor object
### further using list() or chain .limit() to get results
def fetch_mongo(server_addr, db_info, search=None):
	mc = pymongo.Connection(server_addr)
	db = mc[db_info['name']]
	co = db[db_info['collection']]
	return co.find() if not search else co.find()

### extract_anchors function
### extract certain dependency relation according to pre-specified list of pos tags
### e.g.,
### input:  tree and deps of a sentence, pos tags = ['VB', 'JJ', 'NN']
### return <list>
###	e.g,[(u'is', u'VBZ', 8), (u"'ve", u'VBP', 5), (u'do', u'VBP', 7), (u'Yeah', u'JJ', 1), (u'well', u'NN', 2), (u'gotta', u'NN', 6), (u'bowl', u'NN', 11), (u'vinegar', u'NN', 13), (u'put', u'VBN', 9)]
def extract_anchors(deps, tree, targets=['VB']):
	D = defaultdict(list)
	for (i, (w, pos)) in enumerate(tree.pos()):
		# print w, pos
		if not [t for t in targets if t.lower() in pos.lower()]: continue
		D[pos].append( (w, pos, i+1) )
	deps_with_anchors = dict(D).values()
	return [] if not deps_with_anchors else reduce(lambda x,y:x+y, deps_with_anchors)

### _filter_deps_by_rel function
### extract dependency relations which match the target structures
### input:
###		deps <list>: list of dep dictionary object
###			[{'ridx': 1, 'rtoken': u'Yeah', 'ltoken': u'well', 'rel': u'amod', 'lidx': 2},
###			{'ridx': 2, 'rtoken': u'well', 'ltoken': u"'ve", 'rel': u'dobj', 'lidx': 5}]
###		anchor <tuple>: (word, index) pair
###		targets: <list>: target dependency structures, such as [(obj, 1)], to collect obj relation with 1+ occurrence
###	return <list> list of dep dictionary objects
###	e.g., [{'ridx': 2, 'rtoken': u'well', 'ltoken': u"'ve", 'rel': u'dobj', 'lidx': 5}]
def _filter_deps_by_rel(deps, anchor, targets):

	word, idx = anchor

	## anchor: (affects, 3)
	deps_has_anchor_word = [dep for dep in deps if (dep['ltoken'] == word and dep['lidx'] == idx) or (dep['rtoken'] == word and dep['ridx'] == idx)]

	D = defaultdict(list)
	for dep in deps_has_anchor_word:
		collected_rel = [t for (t,r) in targets if t.lower() in dep['rel'].lower()]
		if not collected_rel: continue
		else: collected_rel = collected_rel[0]
		D[collected_rel].append(dep)
	D = dict(D)

	## check number of each relation required
	drop = False
	for (target_rel, target_rel_count) in targets:
		if target_rel_count > 0: # necessary
			if target_rel in D:
				if len(D[target_rel]) < target_rel_count:
					drop = True
			else:
				drop = True
				break

	return [] if drop or not D.values() else reduce(lambda x,y:x+y, D.values())

def _transform_to_tuple(dep): return (dep['rel'], (dep['ltoken'], dep['lidx']), (dep['rtoken'], dep['ridx']))

def _getWordNetPOS(postag):
	if postag.startswith('V'): return wordnet.VERB
	elif postag.startswith('J'): return wordnet.ADJ
	elif postag.startswith('N'): return wordnet.NOUN
	elif postag.startswith('R'): return wordnet.ADV

def extract_and_save(rows, target_postags, target_structures, det_db_cfg, target_word=None, mongodb=True):


	lmtzr = WordNetLemmatizer()


	print 'anchor pos tags:', color.render(', '.join(target_postags), 'lc')
	print 'structures:', color.render(', '.join([x[0]+':'+str(x[1]) for x in target_structures]), 'lc')
	print '='*60
	collect_cnt, skip_cnt = 0, 0	

	mc = pymongo.Connection(det_db_cfg['server_addr'])
	db = mc[det_db_cfg['db']]
	co = db[det_db_cfg['collection']]

	sent_cnt, total_word_cnt, anchor_word_cnt, anchor_word_structure_cnt = 0, 0, 0, 0


	for entry in rows:

		## extract rows
		sid, sent, pos, raw_tree, raw_dep = entry if not mongodb else (entry['id'], entry['sent'], entry['pos'], entry['tree'], entry['dep'])
		
		# read dependency and tree objs
		deps = dependency.read(raw_dep, return_type=dict)
		if not deps: continue
		tree = Tree(raw_tree)


		# collect certain dependency relations according to pre-specified pos tags
		## cdeps: [(u'is', u'VBZ', 8), (u"'ve", u'VBP', 5), (u'do', u'VBP', 7), (u'Yeah', u'JJ', 1), (u'well', u'NN', 2), (u'gotta', u'NN', 6), (u'bowl', u'NN', 11), (u'vinegar', u'NN', 13), (u'put', u'VBN', 9)]
		cdeps = extract_anchors(deps, tree, targets=target_postags)

		## for stat
		sent_cnt += 1
		total_word_cnt += len(tree.pos())
		anchor_word_cnt += len(cdeps)

		##  ('is', 'VBZ', 8) in [(u'is', u'VBZ', 8), (u"'ve", u'VBP', 5), (u'do', u'VBP', 7) ...]
		for (word, pos, idx) in cdeps:

			## check if this is the target word if a target specified
			if target_word and word.lower() != target_word.lower(): continue

			## extract dependency relations which match the target structures 
			rdeps = _filter_deps_by_rel(deps, anchor=(word, idx), targets=target_structures)

			if rdeps: ## got deps match the target structures

				print color.render('(anchor[v]) '+word+'-'+str(idx)+' #'+pos, 'g')

				T = [ _transform_to_tuple(dep) for dep in rdeps]
				for (rel, (l, li), (r, ri)) in T: print '  ',color.render(rel,'r'),color.render('( '+l+'-'+str(li)+', '+r+'-'+str(ri)+' )','y')

				lemma = lmtzr.lemmatize(word, _getWordNetPOS(pos))

				# generate mongo obj
				mongo_obj = {}
				mongo_obj['sid'] = sid 		# sentence id
				mongo_obj['word'] = word 	# anchor word
				mongo_obj['pos'] = pos 		# pos tag of word
				mongo_obj['idx'] = idx 		# word index 
				mongo_obj['deps'] = rdeps	# related deps
				mongo_obj['lemma'] = lemma	# word lemma
				
				co.insert(mongo_obj)

				anchor_word_structure_cnt += 1

	
	mc.close()

	print '='*60
	print 'write statistic log'
	with open('stat.log','w') as fw:
		fw.write('total sent'+'\t'+str(sent_cnt)+'\n')
		fw.write('total word'+'\t'+str(total_word_cnt)+'\n')
		fw.write('anchor word'+'\t'+str(anchor_word_cnt)+'\n')
		fw.write('anchor word with structures'+'\t'+str(anchor_word_structure_cnt)+'\n')


def extract(rows, target_postags, target_structures, target_word=None, mongodb=True, VERBOSE=True):



	print 'anchor pos tags:', color.render(', '.join(target_postags), 'lc')
	print 'structures:', color.render(', '.join([x[0]+':'+str(x[1]) for x in target_structures]), 'lc')
	print '='*60
	collect_cnt, skip_cnt = 0, 0

	for entry in rows:

		## extract rows
		sid, sent, pos, raw_tree, raw_dep = entry if not mongodb else (entry['id'], entry['sent'], entry['pos'], entry['tree'], entry['dep'])
		
		# read dependency and tree objs
		deps = dependency.read(raw_dep, return_type=dict)
		if not deps: continue
		tree = Tree(raw_tree)

		# collect certain dependency relations according to pre-specified pos tags
		## cdeps: [(u'is', u'VBZ', 8), (u"'ve", u'VBP', 5), (u'do', u'VBP', 7), (u'Yeah', u'JJ', 1), (u'well', u'NN', 2), (u'gotta', u'NN', 6), (u'bowl', u'NN', 11), (u'vinegar', u'NN', 13), (u'put', u'VBN', 9)]
		cdeps = extract_anchors(deps, tree, targets=target_postags)

		total_word_cnt += len(tree.pos())
		anchor_word_cnt += len(cdeps)

		##  ('is', 'VBZ', 8) in [(u'is', u'VBZ', 8), (u"'ve", u'VBP', 5), (u'do', u'VBP', 7) ...]
		for (word, pos, idx) in cdeps:

			## check if this is the target word if a target specified
			if target_word and word.lower() != target_word.lower():
				if VERBOSE:
					print color.render('(ancher[x]) '+word+'-'+str(idx)+' #'+pos, 'b')
				continue

			## extract dependency relations which match the target structures 
			rdeps = _filter_deps_by_rel(deps, anchor=(word, idx), targets=target_structures)

			if rdeps: ## got deps match the target structures

				if VERBOSE:
					print color.render('(anchor[v]) '+word+'-'+str(idx)+' #'+pos, 'g')

				T = [ _transform_to_tuple(dep) for dep in rdeps]
				for (rel, (l, li), (r, ri)) in T: print '  ',color.render(rel,'r'),color.render('( '+l+'-'+str(li)+', '+r+'-'+str(ri)+' )','y')

	print '='*60

if __name__ == '__main__':

	######## sqlite version ########
	# db_path = 'data/bnc.db3'
	# sql = "select * from BNC_Parsed where sent like ?"
	# args = ['%'+'interested'+'%']
	# rows = fetch_sqlite(db_path, sql, args)

	######## mongo version ########
	doraemon = 'doraemon.iis.sinica.edu.tw'
	db_info = {'name': 'BNC', 'collection': 'Parsed'}

	# connect to mongo server
	print >> sys.stderr, color.render('fetching data','r'), '...',
	sys.stderr.flush()
	cur = fetch_mongo(doraemon, db_info, None)
	print >> sys.stderr, color.render('done','g')

	# get fetched data
	# print >> sys.stderr, color.render('limiting data','r'), '...',
	# sys.stderr.flush()
	# rows = cur.limit(1000)
	# print >> sys.stderr, color.render('done','g')

	## pre-specified target pos tags
	target_postags = ['JJ', 'VB', 'NN']

	## pre-specified structures
	## 1: necessary
	## 0: optional
	target_structures = [('subj', 0), ('obj', 0), ('prep', 0), ('cop', 0), ('mark', 0)]

	# extract pre-specified targets
	# extract(rows, target_postags, target_structures, target_word='wait')
	# extract(rows, target_postags, target_structures)

	extract_and_save(cur, target_postags, target_structures, det_db_cfg={'server_addr':doraemon, 'db':'BNC', 'collection':'Deps'})

