import sys
import json
import re
from pyspark import SparkContext
from time import gmtime, strftime
import statistics
from pyspark.streaming import StreamingContext
import binascii
import random
import time

def setone(s):
  ss = int(binascii.hexlify(s.encode('utf8')),16)
  return list(set([(ss*a+b)%c%8000 for a,b,c in hash_tables]))


if __name__ == "__main__":
    start = time.time()
    sc = SparkContext.getOrCreate()
    data = sc.textFile(sys.argv[1])
    city_set = set(data.map(lambda x:json.loads(x)["city"]).distinct().collect())
    nh = 2
    hash_tables = list(zip(random.sample(range(-1000,1000), k=nh),random.sample(range(-10001,10000), k=nh),(5801,10007,300491)))
    #hash_tables = [(693,4838,5801),(518,5404,10007)]
    array_bit = set(data.map(lambda x:json.loads(x)["city"]).distinct().filter(lambda x:x!='').flatMap(setone).distinct().collect())
    ans = []
    with open(sys.argv[2],'r') as file:
        for line in file:
            city = json.loads(line)["city"]
            if city != '':
                ss = int(binascii.hexlify(city.encode('utf8')),16)
                if set([(ss*a+b)%c%8000 for a,b,c in hash_tables]).issubset(array_bit):
                    ans.append("1")
                else:
                    ans.append("0")
            else:
                ans.append("1")
    with open(sys.argv[3],"w") as file:
        file.write(" ".join(ans))
    print("array_bit: ",len(array_bit))
    print(94297-sum(map(int,ans)))
    print("size of ans:",len(ans))
    print(hash_tables)
    end = time.time()
    print("Duration: ",end-start)
            
    
