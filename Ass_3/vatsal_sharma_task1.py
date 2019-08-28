from pyspark.context import SparkContext
from pyspark import SparkConf
from itertools import combinations
import random
import time
import sys

def jaccard_similarity(candidate_tuple):
	global business_map

	set_b1 = business_map[candidate_tuple[0]]
	set_b2 = business_map[candidate_tuple[1]]

	intersection_set = len(set_b1.intersection(set_b2))
	union_set = len(set_b1.union(set_b2))

	similarity_value = (float(intersection_set) / float(union_set))

	return(candidate_tuple,similarity_value)

def find_hash(iterator):
	iterator = list(iterator)

	business = iterator[0]
	user = iterator[1]
	output = []

	for i in range(0,total_hash):
		row_permutation = []

		for usr in user:

			row_permute_value = (a[i] * usr + b[i]) % len(users)
			row_permutation.append(row_permute_value)

		min_hash_value = min(row_permutation)

		output.append(min_hash_value)

	return(business,output)

if __name__ == "__main__":

	start = time.time()

	conf = SparkConf()
	conf.setMaster("local[*]")
	sc = SparkContext(conf = conf)

	users_map = {}
	business_map = {}
	total_hash = 150

	path = sys.argv[1]
	output_path = sys.argv[2]

	candidates = []

	csv_data = sc.textFile(path)

	header = csv_data.first()

	input_rdd =  csv_data.filter(lambda line : line != header)\
								 .map(lambda line: line.split(",")) \
								 .filter(lambda line: len(line)>1) \
								 .map(lambda line: (line[0],line[1]))

	users = input_rdd.map(lambda x : x[0]).distinct().collect()

	business_basket = input_rdd.map(lambda x : (x[1],x[0])).groupByKey().map(lambda x : (x[0],set(x[1]))).collect()

	for value in business_basket:

		if value[0] not in business_map:
			business_map[value[0]] = value[1]

	user_id = 0
	for user in users:
		if user not in users_map:
			users_map[user] = user_id
			user_id += 1

	business_user_rdd = input_rdd.map(lambda x : (x[1],users_map[x[0]])).groupByKey()

	a = [random.randint(0, len(users)) for i in range(0, total_hash)]
	b = [random.randint(0, len(users)) for i in range(0, total_hash)]
	r = 3

	signature_matrix = business_user_rdd.map(find_hash).sortByKey().collect()

	band_map = {}

	for value in signature_matrix:

		count = 0
		band = 0
		n = 150

		while count < n:
			
			signature_value = value[1]
			business_value = value[0]
			band_slice = signature_value[count:count + r]
		
			if band not in band_map:
				band_map[band] = [(tuple(band_slice), business_value)]
			else:
				band_map[band].append((tuple(band_slice), business_value))
			band +=  1
			count = count + r
	
	candidates = set()
	
	for value in band_map.values():
		combination = sc.parallelize(value)\
					   .groupByKey()\
					   .map(lambda x : (x[0],list(x[1])))\
					   .filter(lambda x:len(x[1])>=2)\
					   .flatMap(lambda x : list(combinations(x[1],2)))\
					   .collect()
		
		for candidate in combination:
			 candidates.add(candidate)

	candidates = list(candidates)

	print(len(candidates))
	
	candidates_rdd = sc.parallelize(candidates)

	final_result = candidates_rdd.map(lambda candidate_tuple : jaccard_similarity(candidate_tuple)).filter(lambda x : x[1] >= 0.5).collect()

	final_result = sorted(final_result)

	print(len(final_result))

	text_file = open(output_path,"w")
	text_file.write("business_id_1, business_id_2, similarity")
	text_file.write("\n")

	for i in range(0,len(final_result)):
		text_file.write(final_result[i][0][0] + "," + final_result[i][0][1] + "," + str(final_result[i][1]))
		text_file.write("\n")

	print("Duration : " + str(time.time() - start))








