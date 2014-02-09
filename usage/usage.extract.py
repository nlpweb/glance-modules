# -*- coding: utf-8 -*-

db_path = '../data/bnc.dev.db3'

import sqlitedb, dependency

(con, cur) = sqlitedb.connect(db_path)

# id int, sent text, POS text, Tree text, Dep text

res = cur.execute('select * from BNC_Parsed limit 10').fetchall()
rows = list(res)

from nltk import Tree

for (sid, sent, pos, raw_tree, raw_dep) in rows:

	deps = dependency.read(raw_dep)
	tree = Tree(raw_tree)
	