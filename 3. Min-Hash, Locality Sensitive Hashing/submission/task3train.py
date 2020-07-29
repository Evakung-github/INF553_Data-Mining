import sys
from pyspark import SparkContext
import time
import json
import statistics
import random

sc = SparkContext('local[*]', 'task')
def min_hash(iterators,r):
  iterators = list(iterators)
  count = 0
  group = []
  for a,b in hash_tables:
    group.append(min([(i*a+b)%(n_bus+5) for i in iterators]))
    count +=1
    if count % r ==0:
      yield (count // r,tuple(group))
      group = []

        
def find_true(iterators):
    for iter in iterators:
        a,b = iter
        k = len(set(xx.value[a]).intersection(set(xx.value[b])))
        if k>=3 and k/len(set(xx.value[a]).union(set(xx.value[b])))>=0.01:
            yield (a,b)
            

def create_Pair(iterators):
  iterators = list(iterators)
  for i in range(len(iterators)):
    for j in range(i+1,len(iterators)):
      yield (iterators[i],iterators[j])
    
def pearson_corr(iterators):
  for iter in iterators:
    b1,b2 = iter[0]
    b1_score,b2_score = list(zip(*iter[1]))
    b1_mean,b2_mean = statistics.mean(b1_score),statistics.mean(b2_score)#business_avg[b1],business_avg[b2]
    s,t = [i - b1_mean for i in b1_score],[i - b2_mean for i in b2_score]
    if statistics.pstdev(b1_score) == 0 or statistics.pstdev(b2_score)==0:
      continue
    else:
      corr = sum([s[i]*t[i] for i in range(len(b1_score))])
      if corr>0:
        yield (b1,b2,corr/sum([i**2 for i in s])**0.5/sum([i**2 for i in t])**0.5)

if __name__ == '__main__':
    #<train_file> <model _file> <cf_type>
    start = time.time()
    #directory = "../resource/asnlib/publicdata/"
    #data = sc.textFile(directory+"train_review.json")
    cf_type = sys.argv[3]
    
    if cf_type == "item_based":
        data = sc.textFile(sys.argv[1])
        business = data.map(lambda x:json.loads(x)["business_id"]).distinct()
        map_business = dict(zip(business.sortBy(lambda x:x).collect(),range(business.count())))
        map_business_inverse = {map_business[k]:k for k in map_business}

        u = data.map(lambda x:json.loads(x)).map(lambda x:(x["user_id"],(map_business[x["business_id"]],x["stars"]))).repartition(15).cache()
        business_avg = data.map(lambda x:json.loads(x)).map(lambda x:(map_business[x["business_id"]],x["stars"])).aggregateByKey((0,0),lambda x,y:(x[0]+y,x[1]+1),lambda x,y:(x[0]+y[0],x[1]+y[1]))\
                   .mapValues(lambda x:x[0]/x[1]).collectAsMap()

        ans = u.join(u).filter(lambda x:x[1][0][0]>x[1][1][0]).map(lambda x:((x[1][0][0],x[1][1][0]),[x[1][0][1],x[1][1][1]])).groupByKey().filter(lambda x:len(x[1])>=3)\
              .mapPartitions(pearson_corr).filter(lambda x:x[2]>0).map(lambda x:json.dumps({"b1":map_business_inverse[x[0]],"b2":map_business_inverse[x[1]],"sim":x[2]})).collect()

    else:
        data = sc.textFile(sys.argv[1]).map(lambda x:(json.loads(x)['user_id'],json.loads(x)['business_id'],json.loads(x)['stars']))
        bus = data.map(lambda x:x[1]).distinct()
        map_bus = dict(zip(bus.collect(),range(bus.count())))
        map_bus = sc.broadcast(map_bus)
        n_bus = len(map_bus.value)
        user = data.map(lambda x:x[0]).distinct()
        map_user = dict(zip(user.sortBy(lambda x:x).collect(),range(user.count())))
        map_user_inverse = {map_user[k]:k for k in map_user}
        map_user = sc.broadcast(map_user)
        map_user_inverse = sc.broadcast(map_user_inverse)
        user_business = data.map(lambda x:(map_user.value[x[0]],map_bus.value[x[1]])).aggregateByKey([],lambda x,y:x+[y],lambda x,y:x+y).persist()
        xx = sc.broadcast(user_business.collectAsMap())
        nh = 30
        hash_tables = list(zip(random.sample(range(-5000,5000), k=nh),random.sample(range(-5000,5000), k=nh)))
        r = 1
        cand = set(user_business.flatMapValues(lambda x:min_hash(x,r)).map(lambda x:(x[1],x[0])).aggregateByKey([],lambda x,y:x+[y],lambda x,y:x+y).filter(lambda x:len(set(x[1]))>1).map(lambda x:sorted(x[1])).flatMap(create_Pair).distinct().mapPartitions(find_true).collect())


        print("number of cand: ",len(cand))

        b = data.map(lambda x:(x[1],(map_user.value[x[0]],x[2]))).repartition(15)
        b.persist()       
        
        ans = b.join(b).filter(lambda x:(x[1][0][0],x[1][1][0]) in cand).map(lambda x:((x[1][0][0],x[1][1][0]),[x[1][0][1],x[1][1][1]])).aggregateByKey([],lambda x,y:x+[y],lambda x,y:x+y)\
               .mapPartitions(pearson_corr).map(lambda x:json.dumps({"u1":map_user_inverse.value[x[0]],"u2":map_user_inverse.value[x[1]],"sim":x[2]})).collect()
    
    
    print("ans:",len(ans))
    with open(sys.argv[2],"w") as file:
        for i in ans:
            file.write(str(i)+'\n')

    end = time.time()
    print("Duration: ",end-start)
    

    
