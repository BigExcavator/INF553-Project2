import sys
from pyspark import SparkContext
from itertools import combinations
# from collections import Counter
# import time

if __name__ == "__main__":
	if (len(sys.argv)) != 4:
		print "Usage: Haotian_Zhang_son.py baskets.txt .3 output.txt"
		exit(1)

	# start_time = time.time()

	sc = SparkContext(appName = "inf553")

	source = sc.textFile(sys.argv[1])
	support_ratio = float(sys.argv[2])

	num_parts = 2 # number of partitions

	# source = sc.textFile("ratio_0.3.txt", num_parts)
	# support_ratio = 0.3


	# def nextSetCandidate(currentSetCandidate):
	# 	'''
	# 	From x-itemset to x+1-itemset
	# 	e.g: input: [(1,2), (1,3), (2,3), (1,4)]
	# 	output: [(1,2,3)]
	# 	@param currentSetCandidate: x-itemset
	# 	'''

	# 	outcome = list()
	# 	if type(currentSetCandidate[0]) != unicode:
	# 		current_num = len(currentSetCandidate[0])
	# 		next_num = current_num + 1

	# 		temp = list(combinations(currentSetCandidate, next_num))
	# 		for i in temp:
	# 			temp2 = [j for sub in i for j in sub]
	# 			tmpdict = dict(Counter(temp2))
	# 			tmpdict = {k: v for k, v in tmpdict.iteritems() if v == current_num}
	# 			if (len(tmpdict) == next_num):
	# 				outcome.append(tuple(tmpdict.keys()))
	# 	else:
	# 		outcome = combinations(currentSetCandidate, 2)
	# 	return outcome

	def func1(all_baskets):
		'''
		Take in multiple lines of strings that represent baskets.
		Output frequent itemsets in current partition
		@param all_baskets: lines of basket strings
		'''

		outcome = []
		all_baskets = list(all_baskets)

		## single part
		# - aggregate counts
		counts = {} # key: string, val: int of freq count
		num_baskets = 0

		# - produce frequent single items
		for each_basket in all_baskets: # v is each basket in string
			# split each line by comma, and count freq of each item
			each_basket = each_basket.split(',') # each_basket is a list, represents a basket, contains items

			for i in each_basket: # i is each item in a basket
				if i not in counts:
					counts[i] = 1
				else:
					counts[i] = counts[i] + 1
			num_baskets += 1

		threshold = num_baskets * support_ratio

		# - filter out items freq lower than support ratio 
		for i,j in counts.items():
			if j < threshold:
				del counts[i]

		# - get a list of frequent single items
		freq_tuples = counts.keys()
		for i in freq_tuples:
			outcome.append((i, 1))
		freq_singles = freq_tuples
		## single part over
		## after single part, frequent single items, threshold is finalized

		num_items = 2
		while (True):
			# tuple_cand is a generator
			# tuple_cand = nextSetCandidate(freq_tuples)
			tuple_cand = combinations(freq_singles, num_items)
			# create dictonary
			tuple_count = {}
			for i in tuple_cand:
				tuple_count[i] = 0

			for each_basket in all_baskets: # go over all the baskets, each_basket is each basket in string
				each_basket = each_basket.split(',') 
				for each_tuple in tuple_count.keys(): # go over freq itemset candidates, each_tuple is a tuple of itemset
					if set(each_tuple).issubset(set(each_basket)):
					# if (set(each_basket) & set(each_tuple)) == set(each_tuple):
						tuple_count[each_tuple] += 1

			for i,j in tuple_count.items():
				if j < threshold:
					del tuple_count[i]

			if len(tuple_count) == 0: break # if no frequent tuples, break

			freq_tuples = tuple_count.keys()
			for i in freq_tuples:
				outcome.append((i, 1))
			num_items += 1

		return outcome

	# Phase 1: Find candidate itemsets (local frequent)
	candidate_sets = source.mapPartitions(func1).reduceByKey(lambda x,y: 1).collect()
	# print len(candidate_sets)
	# print "phase1 over"
	
	# Phase 2: Collect counts & find global frequent itemsets
	# candidate_sets = [i[0] for i in candidate_sets]

	def func2(all_baskets):
		'''
		Take in multiple lines of strings that represent baskets.
		Output frequent itemsets and their count in current partition
		@param all_baskets: lines of basket strings
		'''

		all_baskets = list(all_baskets)

		counts = {}
		for i in candidate_sets:
			counts[i[0]] = 0

		for each_basket in all_baskets:
			each_basket = each_basket.split(',')
			for each_set in counts.keys():
				if type(each_set) != unicode:
					# if (set(each_basket) & set(each_set)) == set(each_set):
					if set(each_set).issubset(set(each_basket)):
						counts[each_set] += 1
				else:
					if each_set in each_basket:
						counts[each_set] += 1

		return counts.items()
	
	# - calculate the global threshold
	num_baskets_all = source.mapPartitions(lambda x: [len(list(x))]).reduce(lambda x,y: x + y)
	global_threshold = num_baskets_all * support_ratio
	
	phase2Out = source.mapPartitions(func2).reduceByKey(lambda x,y: x + y).filter(lambda x: x[1] >= global_threshold).map(lambda x: x[0]).collect()

	# print len(phase2Out)
	# print "phase2 over"

	text_file = open(sys.argv[3], "w")
	# text_file = open("output.txt", "w")
	for i in phase2Out:
		if type(i) == unicode:
			text_file.write(str(i) + "\n")
		else:
			for j,k in enumerate(i):
				if j != len(i) - 1:
					text_file.write(str(k) + ",")
				else:
					text_file.write(str(k))
			text_file.write("\n")

	# elapsed_time = time.time() - start_time
	# print elapsed_time

	


























