from pyspark.context import SparkContext
from pyspark import SparkConf
from collections import OrderedDict
import sys
import json

conf = SparkConf()
conf.setMaster("local[*]")
sc = SparkContext(conf = conf)
path = sys.argv[1]

output = []
top_20_cust = []
top_20_business = []

review_file = sc.textFile(path)
rdd_json = review_file.map(json.loads).map(lambda x : (x['user_id'],x['business_id'],x['useful'],x['text'],x['stars']))

useful = rdd_json.filter(lambda x: x[2] > 0).count()

stars = rdd_json.filter(lambda x: x[4] == 5.0).count()

longest_review = rdd_json.map(lambda x: len(x[3])).max()

rdd_cust = rdd_json.map(lambda x: (x[0],1))\
				   .reduceByKey(lambda x,y: x + y)\
				   .sortBy(lambda x: (-x[1],x[0]))

rdd_business = rdd_json.map(lambda x: (x[1],1))\
                       .reduceByKey(lambda x,y : x + y)\
                       .sortBy(lambda x: (-x[1],x[0]))

total_customers = rdd_cust.count()
total_business = rdd_business.count()

list_cust = rdd_cust.take(20)

list_business = rdd_business.take(20)

for value in list_cust:
	top_20_cust.append(list(value))

for value in list_business:
	top_20_business.append(list(value))

output.append(('n_review_useful',useful))
output.append(('n_review_5_star',stars))
output.append(('n_characters',longest_review))
output.append(('n_user',total_customers))
output.append(('top20_user',top_20_cust))
output.append(('n_business',total_business))
output.append(('top20_business',top_20_business))


with open(sys.argv[2], 'w') as outfile:
    json.dump(OrderedDict(output), outfile,indent = 2)