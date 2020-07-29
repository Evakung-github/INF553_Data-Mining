import sys
from pyspark import SparkContext
import time
import json
import pickle


def cos_sim(iterators):
  user = iterators["user_id"]
  bus = iterators["business_id"]
  up = user_profile.value[user]
  bp = business_profile.value[bus]
  mul = 0
  for i in bp:
    if i in up:
      mul += up[i]
  return (user,bus,mul/200**0.5/sum([up[i]**2 for i in up])**0.5)

if __name__ == '__main__':
    start = time.time()
    sc = SparkContext('local[*]', 'task2')
    #directory = "../resource/asnlib/publicdata/"
    #val = sc.textFile(directory+"test_review.json")
    val = sc.textFile(sys.argv[1])
    
    with open(sys.argv[2],"rb") as file:
      user_profile,business_profile = pickle.load(file)
    
    user_profile = sc.broadcast(user_profile)
    business_profile = sc.broadcast(business_profile)
    ans = val.map(lambda x:json.loads(x)).filter(lambda x:x["user_id"] in user_profile.value and x["business_id"] in business_profile.value).map(cos_sim).filter(lambda x:x[2]>=0.01)\
          .map(lambda x:json.dumps({"user_id":x[0],"business_id":x[1],"sim":x[2]})).collect()
    
    
    with open(sys.argv[3],"w") as file:
        for i in ans:
            file.write(str(i)+'\n')
    end = time.time()
    print("Duration: ",end-start)