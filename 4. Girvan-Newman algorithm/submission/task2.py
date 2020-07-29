import sys
from pyspark import SparkContext
import random
import time
import json
from itertools import islice
import copy
sys.setrecursionlimit(2000)

sc = SparkContext('local[*]', 'task2')
def bfs_eachnode(n):
  cur = set([n])
  parent = {n:[]}
  cn_shortestpath = {n:1}
  level = []
  while cur:
    next_ = set()
    for c in cur:
      for node in map_adj[c]:
        if node in next_:
          parent[node].append(c)
          cn_shortestpath[node]+=cn_shortestpath[c]
        elif node in parent:
          continue
        else:
          next_.add(node)
          parent[node] = [c]
          cn_shortestpath[node]=cn_shortestpath[c]
    level.append(next_)
    cur = next_

  betweenness = {}
  node_value = {}

  while level:
    leaf = level.pop()
    for node in leaf:
      if node not in node_value:
        node_value[node] = 1
      #credit = node_value[node]/len(parent[node])
      for p in parent[node]:
        credit = cn_shortestpath[p]/cn_shortestpath[node]*node_value[node]
        yield ((min(node,p),max(node,p)),credit)
        #betweenness[(min(node,p),max(node,p))] = credit
        if p not in node_value:
          node_value[p] = credit+1      
        else:
          node_value[p]+=credit

def connected_components(c):
    def traverse(x,visited):
        for i in map_adj[x]:
            if i in visited:
                continue
            visited.add(i)
            visited.update(traverse(i,visited))
        return visited
    cc =[]
    visited = set()
    for i in c:
        if i not in visited:
            temp = traverse(i,set())
            cc.append(tuple(sorted(temp)))
            visited.update(temp)
    return cc

def calModularity(cc):
  if cc in mol:
    return mol[cc]
  mm = 0
  self = 0
  for i in range(0,len(cc)):
    self += (ori_degree.value[cc[i]]*ori_degree.value[cc[i]]/2/m)
    for j in range(i+1,len(cc)):
      if cc[j] in ori_adj.value[cc[i]]:
        mm+=(1-ori_degree.value[cc[i]]*ori_degree.value[cc[j]]/2/m)
      else:
        mm-=(ori_degree.value[cc[i]]*ori_degree.value[cc[j]]/2/m)
  return mm*2-self
        
if __name__ == '__main__':
    #<filter threshold> <input_file_path> <betweenness_output_file_path><community_output_file_path>
    start = time.time()

    sc.setLogLevel("WARN")
    thres = int(sys.argv[1])
    #directory = "../resource/asnlib/publicdata/"
    data = sc.textFile(sys.argv[2]).mapPartitionsWithIndex(lambda idx,row:islice(row,1,None) if idx ==0 else row).map(lambda x:x.split(','))

    user = data.map(lambda x:x[0]).distinct()

    map_user = dict(zip(user.sortBy(lambda x:x).collect(),range(user.count())))
    map_user_inverse = {map_user[k]:k for k in map_user}
    map_user_inverse = sc.broadcast(map_user_inverse)
    map_user = sc.broadcast(map_user)

    b = data.map(lambda x:(x[1],map_user.value[x[0]])).repartition(15).cache()

    edges = b.join(b).filter(lambda x:x[1][0]<x[1][1]).map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=thres)
    m = edges.count()
    map_adj = edges.map(lambda x:x[0]).union(edges.map(lambda x:(x[0][1],x[0][0]))).repartition(30).groupByKey().mapValues(list).collectAsMap()
    between = sc.parallelize(list(map_adj.keys())).flatMap(bfs_eachnode).reduceByKey(lambda x,y:x+y).mapValues(lambda x:x/2).collectAsMap()
    ori_adj = sc.broadcast(copy.deepcopy(map_adj))
    ori_degree = sc.broadcast({k:len(map_adj[k]) for k in map_adj})
    

    ans1 = sc.parallelize(list(map_adj.keys())).flatMap(bfs_eachnode).reduceByKey(lambda x,y:x+y).mapValues(lambda x:x/2).sortBy(lambda x:(-x[1],x[0][0],x[0][1])).map(lambda x:[(map_user_inverse.value[x[0][0]],map_user_inverse.value[x[0][1]]),x[1]]).collect()

    with open(sys.argv[3],"w") as file:
        for i in ans1:
            file.write(", ".join(map(str,i))+'\n')

    cc = connected_components(map_adj.keys())
    print(len(cc))
    mol = {}
    mol = sc.parallelize(cc).map(lambda x:(x,calModularity(x))).collectAsMap()
    max_mol = sum(mol.values())/2/m
    ans2 = cc.copy()
    print("Initial modularity: ",max_mol)

    while between:
      remove_edge = [(k,v) for k,v in sorted(between.items(), key = lambda x:-x[1])][0][0]
      map_adj[remove_edge[0]].remove(remove_edge[1])
      map_adj[remove_edge[1]].remove(remove_edge[0])
      del between[remove_edge]

      for i in cc:
        if remove_edge[0] in i:
          new_between = sc.parallelize(list(i)).flatMap(bfs_eachnode).reduceByKey(lambda x,y:x+y).mapValues(lambda x:x/2).collectAsMap()
          break
      new_cc = connected_components(i)
      between.update(new_between)
      if len(new_cc) ==1:
        continue
      else:
        cc.remove(i)
        cc.extend(new_cc)

        temp_mol = sc.parallelize(cc).map(calModularity).sum()/2/m
        if temp_mol>max_mol:
          print(temp_mol)
          ans2 = cc.copy()
          max_mol = temp_mol


    ans2 = sc.parallelize(ans2).sortBy(lambda x:(len(x),x[0])).map(lambda x:[map_user_inverse.value[i] for i in x]).collect()
    with open(sys.argv[4],"w") as file:
        for i in ans2:
            file.write("'"+"', '".join(map(str,i))+"'"+'\n')
    end = time.time()
    print("Duration: ",end-start) 
