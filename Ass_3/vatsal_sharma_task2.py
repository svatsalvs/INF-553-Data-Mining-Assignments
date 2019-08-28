from pyspark.context import SparkContext
from pyspark.mllib.recommendation import ALS,Rating
from pyspark import SparkConf
import time
import sys
import math

def pearson_correlation(rat_1,rat_2,avg_1,avg_2):

	num = 0
	for i in range(0,len(rat_1)):
		num += (rat_1[i] - avg_1) * (rat_2[i] - avg_2)

	val1 = 0
	for value in rat_1:
		val1 += (value - avg_1) ** 2

	val2 = 0
	for value in rat_2:
		val2 += (value - avg_2) ** 2

	denom = math.sqrt(val1 * val2)

	if denom != 0:
		w = float(num) / float(denom)
	else:
		w = 0.1

	return(w)

def get_similar(entity,hash_map):

	if entity in hash_map:
		output = hash_map[entity]
	else:
		output = []

	return(output)

def predict(iterator):

	iterator = list(iterator)
	user = iterator[0]
	business = iterator[1]

	if user in users_hash_map:
		business_ratings_user = users_hash_map[user]
	else:
		return(user,business,0.0)

	sum_ = 0
	business_ratedby_user = set()
	for i in range(0,len(business_ratings_user)):
		business_ratedby_user.add(business_ratings_user[i][0])
		sum_ += business_ratings_user[i][1]

	avg_rating_user = float(sum_) / float(len(business_ratings_user))

	usr_who_rated_business = get_similar(business,business_bucket_hash_map)

	numerator = 0.0
	denominator = 0.0

	if len(usr_who_rated_business) != 0:
		
		for usr in usr_who_rated_business:
			business_ratings_user_2 = users_hash_map[usr]
			business_ratedby_user_2 = set()

			for i in range(0,len(business_ratings_user_2)):
				business_ratedby_user_2.add(business_ratings_user_2[i][0])

			intersection_business = business_ratedby_user.intersection(business_ratedby_user_2)

			if intersection_business:

				rat_usr1 = []
				rat_usr2 = []
				
				business_ratings_user_dict = dict(business_ratings_user)
				business_ratings_user_2_dict = dict(business_ratings_user_2)

				for bus in intersection_business:
					rat_usr1.append(business_ratings_user_dict[bus])
					rat_usr2.append(business_ratings_user_2_dict[bus])

				avg_usr1 = float(sum(rat_usr1)) / float(len(rat_usr1))
				avg_usr2 = float(sum(rat_usr2)) / float(len(rat_usr2))

				w = pearson_correlation(rat_usr1,rat_usr2,avg_usr1,avg_usr2)
					
				if w != 0:
					denominator = denominator + abs(w)
					rating = float(business_ratings_user_2_dict[business])
					numerator = numerator + ((rating - avg_usr2) * w)

				else:
					numerator += 0.0
					denominator += 0.0

			else:
				numerator += 0.0
				denominator += 0.0

		if (denominator != 0.0):                	
			avg_rating_user = avg_rating_user + float(numerator) / float(denominator)

	return(user,business,avg_rating_user)

def predict_2(iterator):

	iterator = list(iterator)
	user = iterator[0]
	business = iterator[1]

	if business in business_hash_map:
		user_ratings_of_business = business_hash_map[business]

	else:
		return(user,business,0.0)

	sum_ = 0
	user_who_rated_business = set()
	for i in range(0,len(user_ratings_of_business)):
		user_who_rated_business.add(user_ratings_of_business[i][0])
		sum_ += user_ratings_of_business[i][1]

	avg_rating_business = float(sum_) / float(len(user_ratings_of_business))

	business_rated_by_users = get_similar(user,user_bucket_hash_map)

	numerator = 0.0
	denominator = 0.0

	if len(business_rated_by_users) != 0:
		
		for busi in business_rated_by_users:
			user_ratings_of_business_2 = business_hash_map[busi]
			
			user_who_rated_business_2 = set()

			for i in range(0,len(user_ratings_of_business_2)):
				user_who_rated_business_2.add(user_ratings_of_business_2[i][0])

			intersection_user = user_who_rated_business.intersection(user_who_rated_business_2)

			if intersection_user:

				rat_bus1 = []
				rat_bus2 = []
				
				user_ratings_of_business_dict = dict(user_ratings_of_business)
				user_ratings_of_business_2_dict = dict(user_ratings_of_business_2)

				for us in intersection_user:

					rat_bus1.append(user_ratings_of_business_dict[us])
					rat_bus2.append(user_ratings_of_business_2_dict[us])

				avg_bus1 = float(sum(rat_bus1)) / float(len(rat_bus1))
				avg_bus2 = float(sum(rat_bus2)) / float(len(rat_bus2))

				w = pearson_correlation(rat_bus1,rat_bus2,avg_bus1,avg_bus2)

				if w != 0:
					denominator = denominator + abs(w)
					rating = float(user_ratings_of_business_2_dict[user])
					numerator = numerator + ((rating - avg_bus2) * w)
				else:
					numerator += 0.0
					denominator += 0.0

			else:
				numerator += 0.0
				denominator += 0.0

		if (denominator != 0.0):                	
			avg_rating_business = avg_rating_business + float(numerator) / float(denominator)

	return(user,business,avg_rating_business)

if __name__ == "__main__":

	start = time.time()
	conf = SparkConf()
	conf.setMaster("local[*]")
	sc = SparkContext(conf = conf)

	train_data_path = sys.argv[1]
	val_data_path = sys.argv[2] 
	output_path = sys.argv[4] 
	case = int(sys.argv[3])

	if case == 1:

		user_map = {}
		business_map = {}

		train_data = sc.textFile(train_data_path)
		header = train_data.first()
		train_rdd =  train_data.filter(lambda line : line != header)\
							 .map(lambda line: line.split(",")) \
							 .filter(lambda line: len(line)>1) \
							 .map(lambda line: ((line[0],line[1]),float(line[2])))

		test_data = sc.textFile(val_data_path)
		header = test_data.first()
		test_rdd =  test_data.filter(lambda line : line != header)\
							 .map(lambda line: line.split(",")) \
							 .filter(lambda line: len(line)>1) \
							 .map(lambda line: ((line[0],line[1]),float(line[2])))

		train_users = train_rdd.map(lambda x : x[0][0]).distinct().collect()
		train_business = train_rdd.map(lambda x : x[0][1]).distinct().collect()

		test_users = test_rdd.map(lambda x : x[0][0]).distinct().collect()
		test_business = test_rdd.map(lambda x : x[0][1]).distinct().collect()

		user_set = set()
		business_set = set()

		for user in train_users:
			user_set.add(user)
		for user in test_users:
			user_set.add(user)

		for business in train_business:
			business_set.add(business)
		for business in test_business:
			business_set.add(business)

		user_set = list(user_set)
		business_set = list(business_set)

		user_id = 0

		for i in range(0,len(user_set)):
			if user_set[i] not in user_map:
				user_map[user_set[i]] = user_id
				user_id += 1

		business_id = 0

		for i in range(0,len(business_set)):
			if business_set[i] not in business_map:
				business_map[business_set[i]] = business_id
				business_id += 1

		Expected_rating = test_rdd.map(lambda x : ((user_map[x[0][0]],business_map[x[0][1]]),float(x[1])))

		test_data = Expected_rating.map(lambda x : (x[0][0],x[0][1]))

		training_data = train_rdd.map(lambda x : Rating(user_map[x[0][0]],business_map[x[0][1]],float(x[1])))

		model = ALS.train(training_data, rank = 2, iterations = 20, lambda_ = 0.5)

		predictions = model.predictAll(test_data).map(lambda x: ((x[0], x[1]), x[2]))

		prediction_list = predictions.collect()

		sum_predicted_ratings = predictions.values().sum()
		total_count_ratings = predictions.count()

		average_predicted_rating = float(sum_predicted_ratings) / float(total_count_ratings)

		test_data_list = test_data.collect()
		prediction_dict = predictions.collectAsMap()
		prediction_list = predictions.collect()

		missed_predictions = []

		for value in test_data_list:
			if value not in prediction_dict:
				missed_predictions.append(value)

		for value in missed_predictions:
			prediction_list.append((value,average_predicted_rating))

		#predictions = sc.parallelize(prediction_list)
		#ratesAndPreds = Expected_rating.join(predictions)		
		#differences = ratesAndPreds.map(lambda x: abs(x[1][0]-x[1][1])).collect()
		#diff = sc.parallelize(differences)
		#rmse = math.sqrt(diff.map(lambda x: x*x).mean())
		#print(rmse)

		inv_user = {v: k for k, v in user_map.items()}
		inv_business = {v: k for k, v in business_map.items()}

		text_file = open(output_path,"w")
		text_file.write("user_id, business_id, prediction")
		text_file.write("\n")

		for i in range(0,len(prediction_list)):
			text_file.write(inv_user[prediction_list[i][0][0]] + "," + inv_business[prediction_list[i][0][1]] + "," + str(prediction_list[i][1]))
			text_file.write("\n")

	elif case == 2:

		train_data = sc.textFile(train_data_path)
		header = train_data.first()
		train_rdd =  train_data.filter(lambda line : line != header)\
							 .map(lambda line: line.split(",")) \
							 .filter(lambda line: len(line)>1) \
							 .map(lambda line: (line[0],line[1],float(line[2])))

		test_data = sc.textFile(val_data_path)
		header = test_data.first()
		test_rdd =  test_data.filter(lambda line : line != header)\
							 .map(lambda line: line.split(",")) \
							 .filter(lambda line: len(line)>1) \
							 .map(lambda line: (line[0],line[1],float(line[2])))

		missed_pred_rdd = test_rdd.map(lambda x : (x[0],1)).subtract(train_rdd.map(lambda x: (x[0],1)))

		missed_pred_count = missed_pred_rdd.count()

		missed_pred_list = missed_pred_rdd.distinct().collect()

		missed_pred_dict = dict(missed_pred_list)

		business_bucket_hash_map = train_rdd.map(lambda x : (x[1],x[0])).groupByKey().map(lambda x : (x[0],list(x[1]))).collectAsMap()
		
		users_hash_map = train_rdd.map(lambda x : (x[0],(x[1],x[2]))).groupByKey().map(lambda x : (x[0],list(x[1]))).collectAsMap()

		result = test_rdd.map(lambda x : (x[0],x[1])).map(lambda x : predict(x))

		result = result.collect()
		length_result = len(result)

		sum_rating = 0
		for i in range(0,len(result)):

			result[i] = list(result[i])
			
			if result[i][2] > 5.0:
				result[i][2] = 5.0
			elif result[i][2] < 0.0:
				result[i][2] = 0.0

			sum_rating += result[i][2]

			result[i] = tuple(result[i])

		avg = float(sum_rating) / float(length_result - missed_pred_count)

		if missed_pred_dict:
			for i in range(0,len(result)):

				result[i] = list(result[i])
				if result[i][0] in missed_pred_dict:
					result[i][2] = avg

				result[i] = tuple(result[i])

		text_file = open(output_path,"w")
		text_file.write("user_id, business_id, prediction")
		text_file.write("\n")

		for i in range(0,len(result)):
			text_file.write(result[i][0] + "," + result[i][1] + "," + str(result[i][2]))
			text_file.write("\n")

		#result = sc.parallelize(result)
		#joined = test_rdd.map(lambda x:((x[0],x[1]),x[2])).join(result.map(lambda x :((x[0],x[1]),x[2])))
		#RMSE = (joined.map(lambda r: (r[1][0] - r[1][1]) **2).mean()) ** 0.5
		#print(RMSE)

	elif case == 3:

		train_data = sc.textFile(train_data_path)
		header = train_data.first()
		train_rdd =  train_data.filter(lambda line : line != header)\
							 .map(lambda line: line.split(",")) \
							 .filter(lambda line: len(line)>1) \
							 .map(lambda line: (line[0],line[1],float(line[2])))

		test_data = sc.textFile(val_data_path)
		header = test_data.first()
		test_rdd =  test_data.filter(lambda line : line != header)\
							 .map(lambda line: line.split(",")) \
							 .filter(lambda line: len(line)>1) \
							 .map(lambda line: (line[0],line[1],float(line[2])))

		missed_pred_rdd = test_rdd.map(lambda x : (x[1],1)).subtract(train_rdd.map(lambda x: (x[1],1)))

		missed_pred_count = missed_pred_rdd.count()

		missed_pred_list = missed_pred_rdd.distinct().collect()

		missed_pred_dict = dict(missed_pred_list)

		user_bucket_hash_map = train_rdd.map(lambda x : (x[0],x[1])).groupByKey().map(lambda x : (x[0],list(x[1]))).collectAsMap()
		
		business_hash_map = train_rdd.map(lambda x : (x[1],(x[0],x[2]))).groupByKey().map(lambda x : (x[0],list(x[1]))).collectAsMap()

		result = test_rdd.map(lambda x : (x[0],x[1])).map(lambda x : predict_2(x))

		result = result.collect()
		length_result = len(result)

		sum_rating = 0
		for i in range(0,len(result)):

			result[i] = list(result[i])
			
			if result[i][2] > 5.0:
				result[i][2] = 5.0
			elif result[i][2] < 0.0:
				result[i][2] = 0.0

			sum_rating += result[i][2]

			result[i] = tuple(result[i])

		avg = float(sum_rating) / float(length_result - missed_pred_count)

		if missed_pred_dict:
			for i in range(0,len(result)):

				result[i] = list(result[i])
				if result[i][1] in missed_pred_dict:
					result[i][2] = avg

				result[i] = tuple(result[i])

		text_file = open(output_path,"w")
		text_file.write("user_id, business_id, prediction")
		text_file.write("\n")

		for i in range(0,len(result)):
			text_file.write(result[i][0] + "," + result[i][1] + "," + str(result[i][2]))
			text_file.write("\n")

		#result = sc.parallelize(result)
		#joined = test_rdd.map(lambda x:((x[0],x[1]),x[2])).join(result.map(lambda x :((x[0],x[1]),x[2])))
		#RMSE = (joined.map(lambda r: (r[1][0] - r[1][1]) **2).mean()) ** 0.5
		#print(RMSE)

	print("Duration : " + str(time.time() - start))