import os, os.path
import random
import time
from operator import add
import math
import statistics
import sys
from pyspark import SparkContext
import function
from function import addList,addList2
import json

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

def cal_mah(point,stat,sigma):
  diff = 0
  
  for a,b,c in zip(point,stat[1],sigma):
    if stat[0] == 0 or c == 0:
        print("zero: ",a,b,c)
    diff+=((a-b/stat[0])/c)**2
  return math.sqrt(diff)

def findSet(p):
  closest_DS,distance = None,float('inf')
  for i in range(len(DS_set)):
    mah = cal_mah(p,DS_set[i],DS_sigma[i])
    if mah<distance:
      closest_DS = i
      distance = mah
  if distance<std_thres*n_dim**0.5:
    return ("DS",closest_DS)
  closest_CS,distance = None,float('inf')
  for i in cur_cs_group:
    mah = cal_mah(p,CS_set[i],CS_sigma[i])
    if mah<distance:
      closest_CS = i
      distance = mah
  if distance<std_thres*n_dim**0.5:
    return ("CS",closest_CS)
  else:
    return ("RS")

if __name__ == "__main__": 
    starttime = time.time()
    
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("WARN")
    num_file = len(os.listdir(sys.argv[1]))
    initial_data = sc.textFile(sys.argv[1]+"data0.txt").map(lambda x:x.split(',')).map(lambda x:[x[0]]+[float(i) for i in x[1:]])
    cluster_size = int(sys.argv[2])
    std_thres = 2
    #print(initial_data.take(1))
    n_dim =len(initial_data.take(1)[0])-1
    print(n_dim)
    #Initial
    model = function.KMeans(initial_data,3*cluster_size)
    new_assign = model.fit()
    counts = new_assign.groupByKey().mapValues(len).collectAsMap()
    single = [k for k in counts if counts[k] <= 10]
    ones = new_assign.filter(lambda x:x[0] in single).map(lambda x:x[1]).collect()
    non_ones = new_assign.filter(lambda x:x[0] not in single).map(lambda x:x[1])
    run_DS = True
    while run_DS:
        DS = function.KMeans(non_ones,cluster_size).fit()
        print("DS dist: ",DS.groupByKey().mapValues(len).collect())
        counts = DS.groupByKey().mapValues(len).collectAsMap()
        single = [k for k in counts if counts[k] <= 5]
        if len(single) == 0:
            run_DS = False
        else:
            ds_ones = DS.filter(lambda x:x[0] in single).map(lambda x:x[1]).collect()
            non_ones = DS.filter(lambda x:x[0] not in single).map(lambda x:x[1])
            ones.extend(ds_ones)
    memo = DS.map(lambda x:(x[1][0],x[0])).collectAsMap()
    DS_set = DS.map(lambda x:(x[0],x[1][1:])).aggregateByKey([0,[0]*n_dim,[0]*n_dim],lambda x,y:[x[0]+1,addList(x[1],y),addList2(x[2],y)],lambda x,y:[x[0]+y[0],addList(x[1],y[1]),addList(x[2],y[2])]).sortBy(lambda x:x[0]).map(lambda x:x[1]).collect()

    RS_set = []
    CS_set = []
    memo_cs = {}
    cs_merge = {}
    result = []
    cur_cs_group = []
    cs_counter = 0
    if len(ones) >0:
      ones = sc.parallelize(ones)
      RS = function.KMeans(ones,min(cluster_size,ones.count())).fit()
      single = RS.groupByKey().mapValues(len).filter(lambda x:x[1] ==1).map(lambda x:x[0]).collect()
      RS_set = RS.filter(lambda x:x[0] in single).map(lambda x:x[1]).collect()

      cs_temp = RS.filter(lambda x:x[0] not in single).groupByKey().mapValues(list).collect()
      for i in range(len(cs_temp)):
        total,SUM,SQUSM = 0,[0]*n_dim,[0]*n_dim
        memo_cs[i] = []
        cs_merge[i] = [i]
        for j in cs_temp[i][1]:
          total += 1
          SUM = addList(SUM,j[1:])
          SQUSM = addList2(SQUSM,j[1:])
          memo_cs[i].append(j[0])
        CS_set.append([total,SUM,SQUSM])
        cs_counter+=1
      cur_cs_group = list(range(len(CS_set)))

    result.append([len(DS_set),len(memo),len(CS_set),sum([len(memo_cs[i]) for i in memo_cs]),len(RS_set)])
    print("here")
    print(result)

    start = 1
    while start < num_file:
      DS_sigma=[]
      for i in DS_set:
        #print("h1: ",SUMSQ/a-np.square(SUM/a))
        DS_sigma.append(function.cal_sigma(i))
      CS_sigma=[]
      for i in CS_set:
        #print("h2: ",SUMSQ/a-np.square(SUM/a))
        CS_sigma.append(function.cal_sigma(i))
      #print("DS_sigma: ",DS_sigma)
      #print("CS_sigma: ",CS_sigma)
      
        

      dd = sc.textFile(sys.argv[1]+"data"+str(start)+".txt").map(lambda x:x.split(',')).map(lambda x:[x[0]]+[float(i) for i in x[1:]]).map(lambda x:(findSet(x[1:]),x)).persist()

      filter_ds = dd.filter(lambda x:"DS" in x[0]).aggregateByKey((0,[0]*n_dim,[0]*n_dim),lambda x,y:(x[0]+1,addList(x[1],y[1:]),addList2(x[2],y[1:])),lambda x,y:(x[0]+y[0],addList(x[1],y[1]),addList(x[2],y[2])))
      filter_cs = dd.filter(lambda x:"CS" in x[0]).aggregateByKey((0,[0]*n_dim,[0]*n_dim),lambda x,y:(x[0]+1,addList(x[1],y[1:]),addList2(x[2],y[1:])),lambda x,y:(x[0]+y[0],addList(x[1],y[1]),addList(x[2],y[2])))
      filter_rs = dd.filter(lambda x:"RS" in x[0]).map(lambda x:x[1]).collect()

      ds_memo = dd.filter(lambda x:"DS" in x[0]).map(lambda x:(x[1][0],x[0][1])).collectAsMap()
      memo.update(ds_memo)
      cs_memo = dd.filter(lambda x:"CS" in x[0]).map(lambda x:(x[0][1],x[1][0])).groupByKey().mapValues(list).collectAsMap()
      cs_p = 0
      for a in cs_memo:
        cs_p+=len(cs_memo[a])
        memo_cs[a].extend(cs_memo[a])
      # print("count:")
      # print(len(memo),len(memo_cs))
      # print(len(ds_memo),len(cs_memo))

      for (_,num),(a,b,c) in filter_ds.collect():
        DS_set[num][0]+=a
        DS_set[num][1]=addList(DS_set[num][1],b)
        DS_set[num][2]=addList(DS_set[num][2],c)

      for (_,num),(a,b,c) in filter_cs.collect():
        CS_set[num][0]+=a
        CS_set[num][1]=addList(CS_set[num][1],b)
        CS_set[num][2]=addList(CS_set[num][2],c)

      if len(filter_rs)!=0:
        new_RS_set = RS_set + filter_rs

        RS = function.KMeans(new_RS_set,min(10,len(new_RS_set))).fit_nonspark()
        RS_set = []
        cs_temp = []

        for i in RS:
          if len(RS[i]) == 1:
            RS_set+=RS[i]
          else:
            cs_temp.append(RS[i])


        for i in range(len(cs_temp)):
          total,SUM,SQUSM = 0,[0]*n_dim,[0]*n_dim
          memo_cs[cs_counter] = []
          cs_merge[cs_counter] = [cs_counter]
          for j in cs_temp[i]:
            total += 1
            SUM = addList(SUM,j[1:])
            SQUSM = addList2(SQUSM,j[1:])
            memo_cs[cs_counter].append(j[0])
          CS_set.append([total,SUM,SQUSM])
          cur_cs_group.append(cs_counter)
          cs_counter += 1

        a = 0
        if len(CS_sigma) == 0:
            thres = statistics.mean([item for sub in DS_sigma for item in sub])
        else:
          thres = (statistics.mean([item for sub in DS_sigma for item in sub])+statistics.mean([item for sub in CS_sigma for item in sub]))
        while a <len(cur_cs_group):
          i,j,k = CS_set[cur_cs_group[a]].copy()      
          b = a+1
          temp = [0,[0]*n_dim,[0]*n_dim]
          while b <len(cur_cs_group):
            q,w,e = CS_set[cur_cs_group[b]]
            new_N = i+q
            new_sum = [a+b for a,b in zip(j,w)]
            new_sumSQ = [a+b for a,b in zip(k,e)]
            combine_sigma = sum(function.cal_sigma([new_N,new_sum,new_sumSQ]))
            #print(function.cal_sigma(CS_set[cur_cs_group[a]]),function.cal_sigma(CS_set[cur_cs_group[b]]))
            #print(combine_sigma,thres)
            if combine_sigma<=thres:
              cs_merge[cur_cs_group[a]].extend(cs_merge[cur_cs_group[b]])
              temp[0]+=q
              temp[1] = addList(temp[1],w)
              temp[2] = addList(temp[2],e)
              cur_cs_group.remove(cur_cs_group[b])  
              print("cs combine")
              b-=1
            b+=1
          CS_set[cur_cs_group[a]][0]+=temp[0]
          CS_set[cur_cs_group[a]][1]= addList(CS_set[cur_cs_group[a]][1],temp[1])
          CS_set[cur_cs_group[a]][2]= addList(CS_set[cur_cs_group[a]][2],temp[2])
          a+=1   
        print(len(ds_memo),len(cur_cs_group),cs_p+len(new_RS_set)-len(RS_set),len(RS_set))
      else:
        print(len(ds_memo),len(cur_cs_group),cs_p,len(RS_set))

      result.append([len(DS_set),len(memo),len(cur_cs_group),sum([len(memo_cs[i]) for i in memo_cs]),len(RS_set)])
      start += 1
    
    DS_sigma=[]

    for i in DS_set:
      DS_sigma.append(function.cal_sigma(i))

    for f in cur_cs_group:
      closest_DS,distance = None,float('inf')
      a,b,c = CS_set[f]
      for ddd in range(cluster_size):
        mah = cal_mah([i/a for i in b],DS_set[ddd],DS_sigma[ddd])
        #print(mah)
        if mah<distance:
          closest_DS = ddd
          distance = mah
      if distance<std_thres*(n_dim**0.5):
        result[-1][2]-=1
        for csi in cs_merge[f]:
          for csii in memo_cs[csi]:
            memo[csii] = closest_DS
            result[-1][1]+=1
            result[-1][3]-=1
        print("cs merges to ds") 
      else:
        for csi in cs_merge[f]:
          for csii in memo_cs[csi]:
            memo[csii] = -1    

    for i in RS_set:
      memo[i[0]] = -1
    
    print(len(memo))

    with open(sys.argv[3],"w") as file:
        json.dump(memo,file)
    with open(sys.argv[4],"w") as file:
        file.write("round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n")
        for i in range(len(result)):
            file.write(str(i+1)+","+",".join(map(str,result[i]))+'\n')
    end = time.time()
    print("Duration: ",end-starttime)