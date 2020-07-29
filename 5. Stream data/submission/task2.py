import sys
import json
import re
from pyspark import SparkContext
from time import gmtime, strftime
import statistics
from pyspark.streaming import StreamingContext
import binascii
import random


def hashtobit(city):
  ss = int(binascii.hexlify(city.encode('utf8')),16)
  l = [(ss*a+b)%c for a,b,c in hash_tables]
  #l = [(ss*a+b)%c for a,b,c in parameters]
  ans = []
  for i in range(len(l)):
    bitstring = bin(l[i])
    zeros = len(bitstring) - len(bitstring.rstrip('0'))
    ans.append((i,zeros))

  return ans


def combine(rdd):
  if rdd.count()==0:
    return
  true_count = rdd.map(lambda x:len(x[1])).take(1)[0]
  memo = rdd.map(lambda x:pow(2,max(x[1]))).sortBy(lambda x:x).collect()
  g = len(memo)//3
  
  avg = statistics.mean(memo[int(1.2*g):int(1.9*g)])
  #avg = statistics.mean(memo)
  #avg = statistics.median([statistics.mean(memo[i*g:int((i+1)*g)]) for i in range(6)])
  timestring = strftime("%Y-%m-%d %H:%M:%S", gmtime())
  with open(sys.argv[2],"a") as file:
    file.write('{},{},{}\n'.format(timestring,true_count,avg))
  #print(statistics.mean(memo[0:g]),statistics.mean(memo[g:2*g]),statistics.mean(memo[2*g:3*g]))
  #print('{} {} {:.6f} {:12.6f}%'.format(timestring,true_count,avg,(avg/true_count-1)*100), "TRUE" if abs(avg/true_count-1)<0.5 else "False")   


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    batch_dur = 5
    window_length = 30
    sliding_interval = 10
    port = int(sys.argv[1])

    nh = 60
    #hash_tables = list(zip(random.sample(range(-10000,10000), k=nh),random.sample(range(-10000,10000),k=nh),random.sample(range(500000,1000000000), k=nh)))
    hash_tables = [(-5784, 4242, 475528612), (-7445, -8117, 847707908), (5345, -3322, 53081982), (-9547, 1981, 491735196), (-925, -4122, 893730636), (-122, 1759, 536417856), (6497, -465, 932890129), (9193, 7930, 277364510), (2021, -5166, 209345494), (6752, -5263, 61839592), (-2580, 6295, 804406008), (8098, -4693, 747435679), (-7988, 4892, 804824051), (-8854, -9631, 324150832), (-9088, -7386, 137283939), (3161, 4684, 275333552), (1971, 6509, 296193269), (-9310, 9038, 261085849), (9245, -9898, 959071148), (7350, 4179, 392855097), (-4838, 5287, 24670986), (610, 5836, 50023043), (9453, 3586, 1200548), (-6172, 7805, 264695806), (-75, -98, 177749096), (-1481, -9319, 515114864), (1538, -4513, 459014124), (-427, 3347, 105394153), (9563, 6773, 346529937), (-9007, 5215, 833170048), (-5316, -3153, 701832096), (-7881, 6554, 211851653), (8047, 5316, 653508160), (3615, -969, 408839209), (1678, -5874, 964232482), (-9603, -7771, 612835737), (-2613, -7682, 999683604), (-3867, -206, 73595183), (-4841, -1371, 4259718), (-2310, 3912, 775274868), (7567, 9614, 646354995), (-8238, 8253, 844226086), (4501, -1611, 498009778), (9240, 2000, 694905063), (7650, 4727, 68326721), (6351, 1386, 280839009), (-6909, 3520, 957821259), (-1581, -8095, 885523760), (1090, 5516, 254267011), (4288, -7581, 325047909), (-4262, 7348, 3784554), (-7613, -3920, 724353002), (-384, -2708, 395489622), (-8840, 4115, 303185341), (6212, -1195, 991066480), (1213, 4812, 498566989), (-640, -7705, 182088090), (-4553, 5934, 452918094), (2513, 6315, 355348464), (3426, 1234, 304757776)]
   

    ssc=StreamingContext(sc , batch_dur)
    data = ssc.socketTextStream("localhost", port).map(lambda x:json.loads(x)["city"]).transform(lambda x:x.distinct()).window(window_length,sliding_interval).transform(lambda x:x.distinct())\
                  .map(hashtobit).flatMap(lambda x:x).groupByKey().foreachRDD(combine)#count()
    #inputStream.pprint(10)
    with open(sys.argv[2],'w') as file:
      file.write("Time,Ground Truth,Estimation\n")
    #print(hash_tables)

    ssc.start()
    ssc.awaitTermination()
