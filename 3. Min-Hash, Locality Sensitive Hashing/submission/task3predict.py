import sys
from pyspark import SparkContext
import random
import time
import json
import statistics
import math
sc = SparkContext('local[*]', 'task2')

def predict(iterators):
  for iter in iterators:
    active_user,pred_item = iter[0]
    final = []
    iter_ = list(iter[1])
    avg = statistics.mean([i[1] for i in iter_])
    T = len(iter_)
    for sim,score in iter_:
      if tuple(sorted([pred_item, sim],reverse = True)) in model_dict:
        final.append([model_dict[tuple(sorted([pred_item, sim],reverse = True))],score])
    
    top = 5

    final = sorted(final,key = lambda x:x[0],reverse = True)[:top]
    if not final:
      continue
      #yield (active_user,pred_item,3)
    else:

      frac = len(final)/T
      pred_score = sum([(i[0]**1)*i[1] for i in final])/sum([i[0]**1 for i in final])
    
      yield (active_user,pred_item,pred_score*frac+avg*(1-frac))

def predict_user(iterators):
    for iter in iterators:
      final = []
      pred_item,active_user = iter[0]
      for sim,score in iter[1]:
          if tuple(sorted([active_user, sim],reverse = False)) in model_dict:
              final.append([model_dict[tuple(sorted([active_user, sim],reverse = False))],score-user_avg[sim]])
      top = 5
      final = sorted(final,key = lambda x:x[0],reverse = True)[:top]
      if not final:
        continue
      else: 
        frac = math.log2(n_user/n_user_rate_item[pred_item])/math.log2(1000)
        pred_score = sum([(i[0]**1)*i[1] for i in final])/sum([i[0]**1 for i in final])
        yield (active_user,pred_item,user_avg[active_user]+pred_score*min(1,frac))      
                
        

if __name__ == '__main__':
    #<train_file> <test_file> <model_file> <output_file> <cf_type>
    start = time.time()
    data = sc.textFile(sys.argv[1])
    model = sc.textFile(sys.argv[3])
    cf_type = sys.argv[5]
    if cf_type == "item_based":
        val = sc.textFile(sys.argv[2]).map(lambda x:(json.loads(x)["user_id"],json.loads(x)["business_id"])).partitionBy(2)
        user_business = data.map(lambda x:json.loads(x)).map(lambda x:(x["user_id"],(x["business_id"],x["stars"])))
        model_dict = model.map(lambda x:json.loads(x)).map(lambda x:((x["b1"],x["b2"]),x["sim"])).collectAsMap()

        ans = val.join(user_business).map(lambda x:((x[0],x[1][0]),x[1][1])).groupByKey().mapPartitions(predict).map(lambda x:json.dumps({"user_id":x[0],"business_id":x[1],"stars":x[2]})).collect()
    else:
        val = sc.textFile(sys.argv[2]).map(lambda x:(json.loads(x)["business_id"],json.loads(x)["user_id"])).partitionBy(2)
        business_user = data.map(lambda x:json.loads(x)).map(lambda x:(x["business_id"],(x["user_id"],x["stars"])))
        user_avg = data.map(lambda x:json.loads(x)).map(lambda x:(x["user_id"],x["stars"])).aggregateByKey((0,0),lambda x,y:(x[0]+y,x[1]+1),lambda x,y:(x[0]+y[0],x[1]+y[1]))\
               .mapValues(lambda x:x[0]/x[1]).collectAsMap()
        n_user = len(user_avg)
        n_user_rate_item = data.map(lambda x:json.loads(x)).map(lambda x:(x["business_id"],1)).reduceByKey(lambda x,y:x+y).collectAsMap()
        model_dict = model.map(lambda x:json.loads(x)).map(lambda x:((x["u1"],x["u2"]),x["sim"])).collectAsMap()
        ans = val.join(business_user).map(lambda x:((x[0],x[1][0]),x[1][1])).groupByKey().mapPartitions(predict_user).map(lambda x:json.dumps({"user_id":x[0],"business_id":x[1],"stars":x[2]})).collect()
    
    with open(sys.argv[4],"w") as file:
        for i in ans:
            file.write(str(i)+'\n')

    end = time.time()
    print("Duration: ",end-start)   
    
    
    