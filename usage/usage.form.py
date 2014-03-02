# -*- coding: utf-8 -*-

## system 
import sys
from collections import Counter

## nltk
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from nltk.corpus import names

## pymodules
from ListCombination import ListCombination
import color

## mongo
import pymongo

## available mongo servers
doraemon = 'doraemon.iis.sinica.edu.tw'
lost = 'lost.nlpweb.org'

## init
lmtzr = WordNetLemmatizer()
engnames = set(names.words('male.txt') + names.words('female.txt'))

BE = { 're':'are', 's':'is', 'm':'am'}
PER = {'they', 'you', 'we', 'us', 'one', 'he', 'she', 'her', 'him', 'them', 'me'}
PERS = {'their', 'theirs', 'your', 'yours', 'our', 'ours', 'her', 'his', 'my'}

def connect(servers):
	for server in servers:
		try:
			mc = pymongo.Connection(server)
			print >> sys.stderr, '# connected to',server
			break
		except:
			mc = None
			print >> sys.stderr, '# failed to connect',server
	return mc

def construct(words):
	unexpand_pairs = []
	for word, widx, pos, isAnchor in sorted(words, key=lambda x:x[1]):  # sort by widx
		if word.startswith("'") and word[1:] in BE: word = BE[word[1:]]

		word = word if pos.upper()[0] not in ['V', 'N'] else lmtzr.lemmatize(word, convert_to_wn(pos))
		if isAnchor: 
			unexpand_pairs.append( [(word, word, widx, pos, isAnchor, 1.0)] )
		else: 
			unexpand_pairs.append( [(WORD, word, widx, pos, isAnchor, prob) for WORD, prob in categorize(word, pos)] )
	return ListCombination(unexpand_pairs)

def form_mongo_documents(pairs, raw_weight, rule, source):
	documents = []
	for pair in pairs:
		str_pat = ' '.join(['#'+WORD if isAnchor else WORD for (WORD, word, widx, pos, isAnchor, prob) in pair])
		weight = reduce(lambda x,y:x*y, [prob for (WORD, word, widx, pos, isAnchor, prob) in pair])
		raw = ' '.join(['#'+word if isAnchor else word for (WORD, word, widx, pos, isAnchor, prob) in pair])
		document = {}
		for (WORD, word, widx, pos, isAnchor, prob) in pair:
			if isAnchor:
				document['anchor'] = word
				document['pos'] = pos
		document['usage'] = str_pat
		document['weight'] = weight*raw_weight
		document['raw'] = raw
		document['rule'] = ' '.join([x+'#'+str(y) for x,y in rule])
		document['source'] = source
		documents.append(document)
	return documents

def super_sense(word, pos):
	candidates = []
	syns = wn.synsets(word, pos)
	for syn in syns:
		lemma_lst = sorted([(lemma.name, lemma.count()) for lemma in syn.lemmas if lemma.count() > 0], key=lambda x:x[1], reverse=True)
		if not lemma_lst: continue
		lemma_name, count =  lemma_lst[0][0], lemma_lst[0][1]
		lexname = syn.lexname.split('.')[-1] if '.Tops' not in syn.lexname else lemma_name
		candidates.append((lexname.upper(), count))
	if not candidates: return None
	C = Counter()
	S = float(sum([x[1] for x in candidates]))
	for c in candidates:
		C[c[0]] += c[1]/S
	return sorted(C.items(), key=lambda x:x[1], reverse=True)

def categorize(word, pos):

	_word = word.lower()
	_pos = pos.upper()

	# person -exactly
	if word == 'I': return [('PERSON', 1.0)]
	if _word in ['they', 'them']: return [('PERSON', 0.5), ('SOMETHING', 0.5)]

	# one-self -exactly
	if _word.endswith('self'): return [('ONESELF', 1.0)] # and _word != 'self': 
	if _pos.startswith('N') and word in engnames: return [('PERSON', 1.0)]

	## something -exactly
	if _word == 'it': return [('SOMETHING', 1.0)]
	if _pos.startswith('N') and _word in [ 'something', 'anything', 'anyone' ]: return [(word.upper(), 1.0)]

	# person -fuzzy
	if _word in PER: return [('PERSON', 1.0)]
	if _word in PERS: return [("PERSON's", 1.0)]
	if _pos == 'PRP$' : return [("PERSON's", 1.0)]
	if _pos == 'PRP': return [('PERSON', 1.0)]

	## acting -fuzzy
	if _pos == 'VBG' and _word.endswith('ing'): return [('DOING', 1.0)]


	if _pos.startswith('N'): 
		wn_pos = convert_to_wn(_pos)
		if wn_pos == wn.NOUN:
			SS = super_sense(word.lower(), wn_pos)
			if SS: return SS
	# default
	return [(_word, 1.0)]

def convert_to_wn(tb_tag):
	tb_tag = tb_tag.upper()
	if tb_tag.startswith('J'): return wn.ADJ
	elif tb_tag.startswith('V'): return wn.VERB
	elif tb_tag.startswith('N'): return wn.NOUN
	elif tb_tag.startswith('R'): return wn.ADV
	else: return None

def fetch(co, lemma):
	return [r for r in co.find( { 'lemma': lemma, 'patterns': { '$exists': True} } ) if len(r['patterns'])]

def store(co, docs, verbose=True):
	for doc in docs:
		if verbose:
			print '#',color.render('save','g'),'usage >', color.render(doc['usage'], 'lc')
		co.insert(doc)
	
if __name__ == '__main__':

	mc = connect(servers=[lost,doraemon])
	if not mc:
		print 'cannot reach db'
		exit(-1)

	db = mc['BNC']

	# lemma = 'familiar'
	anchors = set(['provide', 'yell', 'agree', 'hear', 'glance', 'separate', 'impress', 'consist', 'listen', 'apply', 'ask', 'aim', 'seek', 'look', 'account', 'launch', 'create', 'build', 'construct', 'contend', 'replace', 'substitute', 'count', 'deal', 'hide'])
	for anchor in anchors:

		print '='*25,'start processing',color.render(anchor,'r'),'='*25
		
		res = fetch(db['Deps'], anchor)

		for entry in res:
			for pat in entry['patterns']:

				## construct usages
				pairs = construct(pat['words'])

				## build mongo documents
				documents = form_mongo_documents(pairs, raw_weight=pat['weight'], rule=pat['rule'], source=entry['_id'])

				## store back to mongo
				store(co=db['usages'], docs=documents, verbose=True)

	mc.close()
