glance-modules
==============

developing modules for glance

* usage > <code>usage.build.py</code>
	
	<pre>
-t	--target	target word, e.g., "familiar"
-r	--rule		self-defined rules. format: python list of tuples
                  e.g., [('subj', 1), ('obj', 0)] # at least one subj, and obj is optional
                  e.g., [('subj', 1), ('cop', 1), ('prep', 1)]
-l	--limit		restrict the number of documents fetched from mongo
-d	--dump		save extracted patterns back to mongo
-h	--help		</pre>
	

	

	
	


