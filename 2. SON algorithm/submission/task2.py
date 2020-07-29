import sys
from pyspark import SparkContext
from itertools import islice
import time


def A_priori(iterators,support):

  count = {}
  #iterators can only be used for one time
  iterators = list(iterators)
  for iter in iterators:
    for i in iter[1]:
      if i in count:
        count[i]+=1
      else:
        count[i] = 1
  prev = [set([k]) for k in count  if count[k]>=support]
  c_items = 2

  while prev:
    yield prev
    candidate = []
    for i in range(len(prev)):
      for j in range(i+1,len(prev)):
        if len(prev[i].intersection(prev[j])) == c_items-2:
          candidate.append(prev[i].union(prev[j]))
    prev = []
    for cand in candidate:
      count = 0
      for iter in iterators:
        if cand.issubset(iter[1]):
          count+=1
          if count>=support:
            #yield(cand)
            prev.append(cand)
            break

    c_items+=1

def partition_red(iterators,candidate):
  #count = {i:0 for i in candidate}
  iterators = list(iterators)
  for cand in candidate:
    count = 0
    for iter in iterators:
      if set(cand).issubset(iter[1]):
        count+=1
    yield (cand,count)


    
if __name__ == '__main__':
  start = time.time()
  sc = SparkContext('local[*]','hw')
  threshold = int(sys.argv[1])
  support = int(sys.argv[2])
  data = sc.textFile(sys.argv[3]).mapPartitionsWithIndex(lambda idx,row:islice(row,1,None) if idx ==0 else row)
  output = sys.argv[4]
  n_partition = data.getNumPartitions()

  sp = (support+(n_partition-1))//n_partition
  candidates = data.map(lambda x:x.split(',')).groupByKey().mapValues(set).filter(lambda x:len(x[1])>threshold).mapPartitions(lambda x:A_priori(x,sp)).flatMap(lambda x:x).map(lambda x:tuple(sorted(list(x)))).distinct()
  c = candidates.collect()

  ans1 = candidates.map(lambda x:(len(x),x)).groupByKey().mapValues(lambda x:sorted(list(x))).sortBy(lambda x:(x[0],x[1])).collect()


  #c = set(candidates.collect())
  frequent = data.map(lambda x:x.split(',')).groupByKey().mapValues(set).filter(lambda x:len(x[1])>threshold).mapPartitions(lambda x:partition_red(x,c)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=support).map(lambda x:(len(x[0]),x[0])).groupByKey().mapValues(lambda x:sorted(list(x))).sortBy(lambda x:(x[0],x[1])).collect()


  with open(output,"w") as file:
    file.write("Candidates:\n")
    for i in ans1:
      if i[0] == 0:
        continue
      file.write(",".join("('{}')".format("', '".join(j)) for j in i[1]))
      file.write('\n\n')
    file.write("Frequent Itemsets:\n")
    for i in frequent:
      if i[0] == 0:
        continue
      file.write(",".join("('{}')".format("', '".join(j)) for j in i[1]))
      file.write('\n\n')
  end = time.time()
  print("Duration: ",end-start)
