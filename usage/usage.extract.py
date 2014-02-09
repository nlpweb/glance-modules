# -*- coding: utf-8 -*-

import sqlitedb, dependency, color, sys # pymodules require
from nltk import Tree
from collections import defaultdict
from pprint import pprint

def fetch(db_path, sql, args=()):
	# connect to database, fetch data
	(con, cur) = sqlitedb.connect(db_path)
	if '?' in sql:
		res = cur.execute(sql, args).fetchall()	# id int, sent text, POS text, Tree text, Dep text
	else:
		res = cur.execute(sql).fetchall()	# id int, sent text, POS text, Tree text, Dep text
	rows = list(res)
	return rows

def extract_anchors(deps, tree, targets=['VB']):
	D = defaultdict(list)
	for (i, (w, pos)) in enumerate(tree.pos()):
		# print w, pos
		if not [t for t in targets if t.lower() in pos.lower()]: continue
		D[pos].append( (w, pos, i+1) )
	deps_with_anchors = dict(D).values()
	return [] if not deps_with_anchors else reduce(lambda x,y:x+y, deps_with_anchors)

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

def extract(rows, target_postags, target_structures):

	print 'anchor pos tags:', color.render(', '.join(target_postags), 'lc')
	print 'structures:', color.render(', '.join([x[0]+':'+str(x[1]) for x in target_structures]), 'lc')
	print '='*60
	collect_cnt, skip_cnt = 0, 0

	for (sid, sent, pos, raw_tree, raw_dep) in rows:
		# read dependency and tree objs
		deps = dependency.read(raw_dep, return_type=dict)
		if not deps: continue

		tree = Tree(raw_tree)

		# collect certain dependency relations according to pre-specified pos tags
		cdeps = extract_anchors(deps, tree, targets=target_postags)

		for (word, pos, idx) in cdeps:
			rdeps = _filter_deps_by_rel(deps, anchor=(word, idx), targets=target_structures)
			if rdeps:
				print color.render('(keep) '+word+'-'+str(idx)+' #'+pos, 'g')
				T = [_transform_to_tuple(dep) for dep in rdeps]
				for (rel, (l, li), (r, ri)) in T:
					print '  ',color.render(rel,'r'),color.render('( '+l+'-'+str(li)+', '+r+'-'+str(ri)+' )','y')
				collect_cnt += 1
			else:
				print '(skip)',word+'-'+str(idx)+' #'+pos
				# if 'worthy' in word:
					# pprint([_transform_to_tuple(dep) for dep in deps])
				skip_cnt += 1

		print '='*60
	print 'total collect:',collect_cnt, '/', skip_cnt+collect_cnt, '\t(',round(collect_cnt/float(skip_cnt+collect_cnt)*100,2),'% )'

if __name__ == '__main__':

	db_path = 'data/bnc.dev.db3'
	sql = "select * from BNC_Parsed where sent like ? limit 2000"
	args = ['%'+'worthy'+'%']
	rows = fetch(db_path, sql, args)

	## pre-specified target pos tags
	target_postags = ['JJ']
	## pre-specified structures
	## +: necessary
	## *: optional
	target_structures = [('subj', 0), ('obj', 0), ('prep', 0)]

	# extract pre-specified targets
	extract(rows, target_postags, target_structures)

