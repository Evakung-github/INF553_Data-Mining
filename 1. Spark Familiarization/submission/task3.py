import sys
import json
import re
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

if __name__ == '__main__':
  review = sc.textFile(sys.argv[1])
  output = sys.argv[2]
  partition_type = sys.argv[3]
  n_partition = int(sys.argv[4])
  n = int(sys.argv[5])
  ans = dict()
  if partition_type == 'default':
    ans['n_partitions'] = review.getNumPartitions()
    ans['n_items'] = review.glom().map(lambda x:len(x)).collect()
    ans['result'] = review.map(lambda x:(json.loads(x)['business_id'],1)).reduceByKey(lambda a,b:a+b).filter(lambda x:x[1]>n).collect()
  else:
    '''
    def reduce(iterators):
        c = {}
        for iter in iterators:
            if iter[0] not in c:
                c[iter[0]]=1
            else:
                c[iter[0]]+=1
        for i in c:
            yield (i,c[i])
    '''
    ans['n_partitions'] = n_partition
    a = review.map(lambda x:(json.loads(x)['business_id'],1)).partitionBy(n_partition).persist()
    ans['n_items'] = a.glom().map(len).collect()
    ans['result'] = a.reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>n).collect()
    
  with open(output,"w") as file:
    file.write(json.dumps(ans))
