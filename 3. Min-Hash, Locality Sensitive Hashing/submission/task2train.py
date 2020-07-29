import sys
from pyspark import SparkContext
from itertools import islice
import random
import time
import re
import json
from collections import Counter
import functools
import operator
import math
import pickle


def tf(iterators):
  #list of lists of words
  memo = Counter(functools.reduce(operator.iconcat, iterators, []))
  max_ = max([memo[k] for k in memo])
  for i in memo:
    memo[i] = memo[i]/max_*idf.value[i]
  
  return [i[0] for i in sorted(memo.items(),key = lambda x:x[1],reverse=True)[:200]]

def user_count(iterators):
  iterators = list(iterators)
  n = len(iterators)
  memo = Counter(functools.reduce(operator.iconcat, iterators, []))
  for i in memo:
    memo[i] = 1
    #memo[i] /= n
  return memo
  #return {k:v for k,v in sorted(memo.items(),key = lambda x:x[1],reverse = True)[:300]}


if __name__ == '__main__':
    start = time.time()
    sc = SparkContext('local[*]', 'task2')
    #directory = "../resource/asnlib/publicdata/"
    #data = sc.textFile(directory+"train_review.json")
    data = sc.textFile(sys.argv[1])
    stopwords = set()
    with open(sys.argv[3]) as file:
            for i in file:
                stopwords.add(i.split()[0])
    stopwords.add("")

    business = data.map(lambda x:(json.loads(x)["business_id"],re.split("[^a-zA-Z]+",json.loads(x)["text"].lower())))\
               .mapValues(lambda x:[i for i in x if i not in stopwords]).partitionBy(15)
    rare_words = set(business.flatMap(lambda x:x[1]).map(lambda x:(x.strip(),1)).reduceByKey(lambda x,y: x+y).filter(lambda x:x[1]<=30).map(lambda x:x[0]).collect())

    business_filter=business.mapValues(lambda x:[i for i in x if i not in rare_words]).groupByKey().persist()
    bus_N = business_filter.count()
    idf = sc.broadcast(business_filter.flatMap(lambda x:list(set(functools.reduce(operator.iconcat, x[1], [])))).map(lambda x:(x,1))\
          .reduceByKey(lambda x,y:x+y).mapValues(lambda x:math.log2(bus_N/x)).collectAsMap())
    features = set(business_filter.mapValues(tf).flatMap(lambda x:x[1]).distinct().collect())
    bussiness_profile = business_filter.mapValues(tf).collectAsMap()

    print("features: ",time.time()-start)

    user = data.map(lambda x:(json.loads(x)["user_id"],re.split("[^a-zA-Z]+",json.loads(x)["text"].lower())))\
               .mapValues(lambda x:[i for i in x if i in features]).persist()
    user_profile = user.groupByKey().mapValues(user_count).collectAsMap()
    
    with open(sys.argv[2],"wb") as file:
        pickle.dump([user_profile,bussiness_profile],file)


    print("Duration: ",time.time()-start)