from pyspark.context import SparkContext
from pyspark import SparkConf
from itertools import combinations
import copy
import time
import sys

def matrix(iterator):
	iterator = list(iterator)
	yield (iterator[0], iterator[1])
	yield (iterator[1], iterator[0])

def girvan_newman(iterator):
	global adjacency_matrix
	root_node = iterator
	visited = set()
	active_node_Q = [root_node]
	predicted = {}
	bfs_tree = []
	temp_node_dict = {}
	temp_edge_dict = {}
	shortest_path = {}
	shortest_path[root_node] = 1

	while active_node_Q:
		v = active_node_Q.pop(0)
		if v == root_node:
			predicted[root_node] = [[],0]
			visited.add(root_node)
			bfs_tree.append(root_node)
		else:
			visited.add(v)
			bfs_tree.append(v)

		v_root_distance = predicted[v][1]

		to_visit = adjacency_matrix[v] - visited

		unique_set = to_visit - set(active_node_Q)

		for node in unique_set:
			active_node_Q.append(node)

		for node in to_visit:

			if node in predicted:
				if predicted[node][1] == v_root_distance + 1:
					predicted[node][0].append(v)
					shortest_path[node] += shortest_path[v]
			else:
				predicted[node] = [[v],predicted[v][1] + 1]
				shortest_path[node] = shortest_path[v]

	while bfs_tree:
		node = bfs_tree.pop()

		if node not in temp_node_dict:
			temp_node_dict[node] = 1

		if node in predicted:
			parent = predicted[node][0]
			total = 0.0
			for par in parent:
				total += shortest_path[par]
			for par in parent:
				if total > 0.0:
					edge_contribution = float(temp_node_dict[node]) * (float(shortest_path[par]) / float(total))
				else:
					edge_contribution = float(temp_node_dict[node])
				temp_edge_dict[(node,par)] = edge_contribution
				if par not in temp_node_dict:
					temp_node_dict[par] = 1
				temp_node_dict[par] += edge_contribution

	output = []

	for key,value in temp_edge_dict.items():
		output.append((key,value))

	return(output)

def girvan_newman_2(iterator):
	global new_adjacency_matrix
	root_node = iterator
	visited = set()
	active_node_Q = [root_node]
	predicted = {}
	bfs_tree = []
	temp_node_dict = {}
	temp_edge_dict = {}
	shortest_path = {}
	shortest_path[root_node] = 1

	while active_node_Q:
		v = active_node_Q.pop(0)
		if v == root_node:
			predicted[root_node] = [[],0]
			visited.add(root_node)
			bfs_tree.append(root_node)
		else:
			visited.add(v)
			bfs_tree.append(v)

		v_root_distance = predicted[v][1]

		to_visit = new_adjacency_matrix[v] - visited

		unique_set = to_visit - set(active_node_Q)

		for node in unique_set:
			active_node_Q.append(node)

		for node in to_visit:

			if node in predicted:
				if predicted[node][1] == v_root_distance + 1:
					predicted[node][0].append(v)
					shortest_path[node] += shortest_path[v]
			else:
				predicted[node] = [[v],predicted[v][1] + 1]
				shortest_path[node] = shortest_path[v]

	while bfs_tree:
		node = bfs_tree.pop()

		if node not in temp_node_dict:
			temp_node_dict[node] = 1

		if node in predicted:
			parent = predicted[node][0]
			total = 0.0
			for par in parent:
				total += shortest_path[par]
			for par in parent:
				if total > 0.0:
					edge_contribution = float(temp_node_dict[node]) * (float(shortest_path[par]) / float(total))
				else:
					edge_contribution = float(temp_node_dict[node])
				temp_edge_dict[(node,par)] = edge_contribution
				if par not in temp_node_dict:
					temp_node_dict[par] = 1
				temp_node_dict[par] += edge_contribution

	output = []

	for key,value in temp_edge_dict.items():
		output.append((key,value))

	return(output)

def bfs(active_node,matrix):
	global new_adjacency_matrix
	visited = set()
	bfs_tree = []

	while active_node:
		v = active_node.pop(0)
		visited.add(v)
		bfs_tree.append(v)

		to_visit = matrix[v] - visited
		unique_set = to_visit - set(active_node)

		for node in unique_set:
			active_node.append(node)

	return (bfs_tree)

def calculate_modularity(communities):
	mod_value = 0.0
	for com in communities:
		mod = 0.0
		for comb in combinations(com,2):
			if comb[0] not in adjacency_matrix[comb[1]]:
				aij = 0
			else:
				aij = 1
			ki = len(adjacency_matrix[comb[0]])
			kj = len(adjacency_matrix[comb[1]])
			mod = mod + (aij - (float(ki * kj) / (2 * total_edges)))
		mod_value += mod

	return(mod_value)

def get_community(communities_list,edge):
	global new_adjacency_matrix
	global total_edges

	node_1 = edge[0]
	node_2 = edge[1]

	new_adjacency_matrix[node_1] = new_adjacency_matrix[node_1] - set([node_2])
	new_adjacency_matrix[node_2] = new_adjacency_matrix[node_2] - set([node_1])

	if len(new_adjacency_matrix[node_1]) > len(new_adjacency_matrix[node_2]):
		check_com = new_adjacency_matrix[node_1]
		find_node = node_2
	else:
		check_com = new_adjacency_matrix[node_2]
		find_node = node_1

	for node in check_com:
		if find_node in new_adjacency_matrix[node]:
			return(communities_list,None)

	active_node = [node_2]

	bfs_tree = bfs(active_node,new_adjacency_matrix)

	if node_1 in bfs_tree:
		return(communities_list,None)
	else:
		new_communities = []
		for com in communities_list:
			if set(edge).issubset(set(com)):
				split_two_com = [set(com) - set(bfs_tree),set(bfs_tree)]
				new_communities.extend(split_two_com)
			else:
				new_communities.append(com)

		mod_value = calculate_modularity(new_communities)

		final_mod = float(mod_value) / (2 * total_edges)

		return(new_communities,final_mod)

if __name__ == "__main__":

	start = time.time()

	conf = SparkConf()
	conf.setMaster("local[*]")
	sc = SparkContext(conf = conf)

	path = sys.argv[2] #"sample_data.csv"
	output_path_between = sys.argv[3] #"vatsal_betweenness.txt"
	output_path_community = sys.argv[4] #"vatsal_community.txt"
	threshold = int(sys.argv[1]) #7

	csv_data = sc.textFile(path)

	header = csv_data.first()

	input_rdd = csv_data.filter(lambda line: line != header) \
		.map(lambda line: line.split(",")) \
		.filter(lambda line: len(line) > 1) \
		.map(lambda line: (line[0], line[1])) \
		.groupByKey() \
		.filter(lambda x: len(x[1]) >= threshold) \
		.map(lambda x: (x[0], set(x[1])))

	user_business_dict = input_rdd.collectAsMap()

	users_list = input_rdd.map(lambda x: x[0]).collect()

	user_pairs = list(combinations(users_list, 2))

	final_pairs = []

	for pair in user_pairs:

		business_set_1 = user_business_dict[pair[0]]
		business_set_2 = user_business_dict[pair[1]]
		intersection = business_set_1.intersection(business_set_2)

		if len(intersection) >= threshold:
			final_pairs.append(pair)

	adjacency_matrix = sc.parallelize(final_pairs) \
		.flatMap(matrix) \
		.groupByKey() \
		.map(lambda x: (x[0], set(x[1]))) \
		.collectAsMap()

	adjacency_matrix_keys = adjacency_matrix.keys()

	result = sc.parallelize(adjacency_matrix_keys) \
		.flatMap(girvan_newman) \
		.map(lambda x: (tuple(sorted(x[0])), x[1])) \
		.groupByKey() \
		.mapValues(lambda x: sum(x) / 2)

	out = result.sortBy(lambda x: (-x[1], x[0][0], x[0][1])) \
		.collect()

	text_file = open(output_path_between,"w")

	for i in range(0,len(out)):
		text_file.write(str(out[i][0]) + ", " + str(out[i][1]))
		text_file.write("\n")

	text_file.close()

	sorted_edges = result.sortBy(lambda x: -x[1]).collect()
	new_adjacency_matrix = copy.deepcopy(adjacency_matrix)
	present_communities_list = [set(adjacency_matrix.keys())]
	final_com = []
	total_edges = len(sorted_edges)
	initial_community = []

	while present_communities_list[0]:

		root = list(present_communities_list[0])[0]
		active_node_Q = [root]

		bfs_tree = bfs(active_node_Q,adjacency_matrix)

		present_communities_list = [present_communities_list[0] - set(bfs_tree)]
		initial_community.append(set(bfs_tree))
	
	present_communities_list = initial_community

	mod_value = calculate_modularity(present_communities_list)

	final_mod = float(mod_value) / (2 * total_edges)

	max_mod = final_mod

	while sorted_edges:
		present_communities_list,mod_value = get_community(present_communities_list, sorted_edges[0][0])

		if mod_value:
			if max_mod < mod_value:
				max_mod = mod_value
				final_com = present_communities_list

		sorted_edges = sc.parallelize(new_adjacency_matrix.keys()) \
			.flatMap(girvan_newman_2) \
			.map(lambda x: (tuple(sorted(x[0])), x[1])) \
			.groupByKey() \
			.mapValues(lambda x: sum(x) / 2) \
			.sortBy(lambda x: -x[1]) \
			.collect()

	final_com = sc.parallelize(final_com)

	ans_com = final_com.map(lambda x:sorted(x))\
				   .sortBy(lambda x:(len(x),list(x[0])))\
				   .collect()

	print(len(ans_com))
	print(len(out))

	text_file = open(output_path_community,"w")
	
	for i in range(0,len(ans_com)):
		for j in range(0,len(ans_com[i])):
			if j != len(ans_com[i]) - 1:
				text_file.write("\'"+ str(ans_com[i][j])+ "\'"+ ", ")
			else:
				text_file.write("\'"+str(ans_com[i][j])+"\'")
		text_file.write("\n")
	
	text_file.close()

	print("Duration : " + str(time.time() - start))