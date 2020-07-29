import sys
from pyspark import SparkContext
from itertools import islice
import random
import time
import json
sc = SparkContext('local[*]', 'task2')

def min_hash(iterators,r):
  iterators = list(iterators)
  count = 0
  group = []
  for a,b in hash_tables:
    group.append(min([(i*a+b)%(n_user+1) for i in iterators]))
    count +=1
    if count % r ==0:
      yield (count // r,tuple(group))
      group = []


def create_Pair(iterators):
  iterators = list(iterators)
  for i in range(len(iterators)):
    for j in range(i+1,len(iterators)):
      yield (iterators[i],iterators[j])


if __name__ == '__main__':
  start = time.time()
  data = sc.textFile(sys.argv[1]).map(lambda x:(json.loads(x)['user_id'],json.loads(x)['business_id']))
  #data = sc.textFile("train_review.json").map(lambda x:(json.loads(x)['user_id'],json.loads(x)['business_id']))
  user = data.map(lambda x:x[0]).distinct()
  map_user = dict(zip(user.collect(),range(user.count())))
  n_user = user.count()
  business = data.map(lambda x:x[1]).distinct()
  map_business = dict(zip(business.sortBy(lambda x:x).collect(),range(business.count())))
  map_business_inverse = {map_business[k]:k for k in map_business}
  data = data.map(lambda x:(map_business[x[1]],map_user[x[0]])).groupByKey().persist()
  xx = data.collectAsMap()
  nh = 50
  hash_tables = list(zip(random.sample(range(-5000,5000), k=nh),random.sample(range(-5000,5000), k=nh)))
  r = 1
  similar_groups = data.flatMapValues(lambda x:min_hash(x,r)).map(lambda x:(x[1],x[0])).groupByKey().filter(lambda x:len(set(x[1]))>1)
  
  candidate_pairs = similar_groups.map(lambda x:sorted(x[1])).flatMap(create_Pair).distinct()
  c = candidate_pairs.count()

  ans = candidate_pairs.map(lambda x:(x,len(set(xx[x[0]]).intersection(set(xx[x[1]])))/len(set(xx[x[0]]).union(set(xx[x[1]])))))\
                .filter(lambda x:x[1]>=0.05).map(lambda x:json.dumps({"b1":map_business_inverse[x[0][0]],"b2":map_business_inverse[x[0][1]],"sim":x[1]})).collect()
  print("candidate_pairs: ",c)
  print("ans: ",len(ans))
  with open(sys.argv[2],"w") as file:
    for i in ans:
      file.write(str(i)+'\n')

  end = time.time()
  print("Duration: ",end-start)
