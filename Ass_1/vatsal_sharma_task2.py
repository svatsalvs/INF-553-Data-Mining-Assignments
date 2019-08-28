from pyspark.context import SparkContext
from pyspark import SparkConf
from collections import OrderedDict
import time
import sys
import json

conf = SparkConf()
conf.setMaster("local[*]")
sc = SparkContext(conf = conf)
path_review = sys.argv[1]
path_business = sys.argv[2]

output = []

review_file = sc.textFile(path_review)
business_file = sc.textFile(path_business)

rdd_review_json = review_file.map(json.loads).map(lambda x: (x['business_id'],x['stars']))
rdd_business_json = business_file.map(json.loads).map(lambda x: (x['business_id'],x['state']))

rdd_join_review_business = rdd_review_json\
	                       .join(rdd_business_json)\
	                       .map(lambda x : (x[1][1],x[1][0]))\
                           .groupByKey()\
                           .mapValues(lambda x : sum(x) / len(x))\
                           .sortBy(lambda x: (-x[1],x[0]))

list_stars_state = rdd_join_review_business.collect()

text_file = open(sys.argv[3], "a")
text_file.write("state,stars")
text_file.write("\n")
text_file.close()

text_file = open(sys.argv[3], "a")

for value in list_stars_state:
	text_file.write(str(value[0]) + "," + str(value[1]))
	text_file.write("\n")

text_file.close()

start = time.time()
second_method_list = rdd_join_review_business.collect()
for i in range(0,5):
	print(second_method_list[i])
end = time.time() - start

output.append(("m1",end))

start = time.time()
first_methos_list = rdd_join_review_business.take(5)
for value in first_methos_list:
	print(value)
end = time.time() - start

output.append(("m2",end))

explanation = "Method 1 takes more time than method 2 because in method 1 we collect the entire data first and then print the first 5 states where as in method 2 we just take first 5 states from sorted rdd into a list and print it."

output.append(("explanation",explanation))

with open(sys.argv[4], 'w') as outfile:
    json.dump(OrderedDict(output), outfile,indent = 2)