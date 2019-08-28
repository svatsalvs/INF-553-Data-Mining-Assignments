from pyspark.context import SparkContext
from pyspark import SparkConf
from itertools import combinations
import time
import sys

def find_frequent_item_set(candidate_set,baskets,singleton):

    chunk_total_baskets = len(baskets)
    chunk_support_threshold = support_threshold * (float(chunk_total_baskets) / float(total_baskets))
    frequent_dict = {}
    frequent_set = set()

    if singleton == True:

        for basket in baskets:      
            for item in basket:
            
                if item in frequent_dict:
                    frequent_dict[item] += 1

                else:
                    frequent_dict[item] = 1

        for item,value in frequent_dict.items():

            if value >= chunk_support_threshold:

                frequent_set.add(item)

    else:

        for basket in baskets:

            temp_basket = set(basket)

            for item in candidate_set:

                temp_item = set(item)
                key = tuple(sorted(temp_item))

                if temp_item.issubset(temp_basket):

                    if key in frequent_dict:

                        frequent_dict[key] += 1

                    else:

                        frequent_dict[key] = 1

        for key,value in frequent_dict.items():

            if value >= chunk_support_threshold:

                frequent_set.add(key)

    return(sorted(frequent_set))

def find_candidates(frequent_item_set,size,pair_boolean):

    candidate_set = []

    if pair_boolean == True:

        candidate_set = list(combinations(frequent_item_set,size))
        return(candidate_set)
    
    else:

        for i in range(0,len(frequent_item_set) - 1):
            for j in range(i+1,len(frequent_item_set)):
    
                if len(set(frequent_item_set[i]) - set(frequent_item_set[j])) == 1:

                    probable_candidate = tuple(sorted(set(frequent_item_set[i]) | set(frequent_item_set[j])))

                    if probable_candidate not in candidate_set:

                        comb_probable_candidate = combinations(probable_candidate,size-1)
                        comb_probable_candidate_set = set(comb_probable_candidate)
                        previousfrequents_set = set(frequent_item_set)
                        if comb_probable_candidate_set.issubset(previousfrequents_set):
                            candidate_set.append(probable_candidate)

        return(candidate_set)

def phase_1_map(iterator):

    baskets = []
    result_frequent_item_set = []
    candidate_set = []

    for basket in iterator:
        baskets.append(basket)

    size_1_frequent_itemsets = find_frequent_item_set(candidate_set,baskets,singleton = True)

    old_frequent_item_set = size_1_frequent_itemsets

    size_of_item_set = 2 

    pair_boolean = False

    while len(old_frequent_item_set) != 0:

        result_frequent_item_set.extend(list(old_frequent_item_set))

        if size_of_item_set == 2:
            pair_boolean = True

        candidate_set = find_candidates(old_frequent_item_set,size_of_item_set,pair_boolean)
        pair_boolean = False
        new_frequent_item_set = find_frequent_item_set(candidate_set,baskets,singleton = False)
        
        old_frequent_item_set = new_frequent_item_set
        size_of_item_set += 1

    return(result_frequent_item_set)


def phase_2_map(iterator):

    global result_1

    local_list = []
    local_dict = {}

    for basket in iterator:
        temp_basket = set(basket)

        for item in result_1:
            
            if type(item) != tuple:
                temp_item = set([item])
                if temp_item.issubset(temp_basket):
                    if item in local_dict:
                        local_dict[item] += 1
                    else:
                        local_dict[item] = 1
            else:
                temp_item = set(item)
                key = tuple(sorted(temp_item))
                if temp_item.issubset(temp_basket):
                    if key in local_dict:
                        local_dict[key] += 1
                    else:
                        local_dict[key] = 1

    for key,value in local_dict.items():
        local_list.append(tuple([key,value]))

    return(local_list)



if __name__ == "__main__":

    start = time.time()

    conf = SparkConf()
    conf.setMaster("local[*]")
    sc = SparkContext(conf = conf)
    path = sys.argv[3]
    output_path = sys.argv[4]
    support_threshold = int(sys.argv[2])
    case = int(sys.argv[1])

    csv_data = sc.textFile(path)

    header = csv_data.first()

    if case == 1:

        main_baskets =  csv_data.filter(lambda line : line != header)\
                        .map(lambda line: line.split(",")) \
                        .filter(lambda line: len(line)>1) \
                        .map(lambda line: (line[0],line[1]))\
                        .groupByKey()\
                        .map(lambda x : (x[0] , set(x[1])))\
                        .map(lambda x : x[1])

    else:

        main_baskets =  csv_data.filter(lambda line : line != header)\
                        .map(lambda line: line.split(",")) \
                        .filter(lambda line: len(line)>1) \
                        .map(lambda line: (line[1],line[0]))\
                        .groupByKey()\
                        .map(lambda x : (x[0] , set(x[1])))\
                        .map(lambda x : x[1])

    total_baskets = main_baskets.count()

    output_1 = {}
    output_2 = {}
    result_1 = []
    result_2 = []

    result_1 = main_baskets.mapPartitions(phase_1_map).distinct().collect()

    result_2 = main_baskets.mapPartitions(phase_2_map).reduceByKey(lambda x,y : x+y).filter(lambda x : x[1] >= support_threshold).map(lambda x : x[0]).collect()

    result_1 = sorted(result_1,key = lambda x : x[0])

    result_2 = sorted(result_2,key = lambda x : x[0])

    for value in result_1:
        if type(value) == tuple:
            if len(value) in output_1:
                output_1[len(value)].append(value)
            else:
                output_1[len(value)] = [value] 
        else:
            if 1 in output_1:
                output_1[1].append(value)
            else:
                output_1[1] = [value]

    for value in result_2:
        if type(value) == tuple:
            if len(value) in output_2:
                output_2[len(value)].append(value)
            else:
                output_2[len(value)] = [value] 
        else:
            if 1 in output_2:
                output_2[1].append(value)
            else:
                output_2[1] = [value]

    text_file = open(output_path, "w")
    text_file.write("Candidates:")
    text_file.write("\n")

    for key,value in sorted(output_1.items()):

        temp_list = sorted(value)

        if key == 1:

            for i in range(0,len(temp_list) - 1):
                text_file.write("(" + "\'" + temp_list[i] + "\'" + "),")

            text_file.write("(" + "\'" + temp_list[len(temp_list) - 1] + "\'" + ")")

            text_file.write("\n")
            text_file.write("\n")
        else:

            for i in range(0,len(temp_list) - 1):
                text_file.write(str(temp_list[i]) + ",")

            text_file.write(str(temp_list[len(temp_list) - 1]))
            
            text_file.write("\n")
            text_file.write("\n")

    text_file.write("Frequent Itemsets:")
    text_file.write("\n")

    for key,value in sorted(output_2.items()):

        temp_list = sorted(value)

        if key == 1:

            for i in range(0,len(temp_list) - 1):
                text_file.write("(" + "\'" + temp_list[i] + "\'" + "),")

            text_file.write("(" + "\'" + temp_list[len(temp_list) - 1] + "\'" + ")")

            text_file.write("\n")
            text_file.write("\n")
        
        else:

            for i in range(0,len(temp_list) - 1):
                text_file.write(str(temp_list[i]) + ",")

            text_file.write(str(temp_list[len(temp_list) - 1]))
            
            if key != sorted(output_2.keys())[-1]:

                text_file.write("\n")
                text_file.write("\n")

    text_file.close()

    end = time.time() - start
    print("Duration: " + str(end))