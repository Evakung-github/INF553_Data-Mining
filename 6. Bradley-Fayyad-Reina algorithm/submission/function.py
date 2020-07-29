import os, os.path
import random
from operator import add
import math
import statistics

def find_closest(x,centroids):
  distance = float('inf')
  m  = 100 + 1
  for j,cen in enumerate(centroids):
    #print("x: {} cen:{}".format(len(x),len(cen)))
    d = math.sqrt(sum([(a-b)**2 for a,b in zip(x[1:],cen)]))
    if d < distance:
      distance = d
      m = j
  return (m,x)

def addList(x,y):
  return [a+b for a,b in zip(x,y)]

def addList2(x,y):
  return [a+b**2 for a,b in zip(x,y)]

def find_closest_dist(x,centroids):
  distance = 0
  fp = None
  x = list(x)
  for p in x:
    dd = float('inf')
    for cen in centroids:
      d = math.sqrt(sum([(a-b)**2 for a,b in zip(p[1:],cen)]))
      if d < dd:
        dd = d
    if dd>distance:
      distance = dd
      fp = p
  yield (fp,distance)


class KMeans:
  def __init__(self,data,n_cluster):
    self.data = data
    self.n_cluster = n_cluster

  def find_closest(self,x,centroids):
    distance = float('inf')
    m  = self.n_cluster + 1
    for j,cen in enumerate(centroids):

      d = math.sqrt(sum([(a-b)**2 for a,b in zip(x[1:],cen)]))
      if d < distance:
        distance = d
        m = j
    return (m,x)

  def initialPoint(self):
    centroids = self.data.map(lambda x:x[1:]).takeSample(False,1)
    for _ in range(self.n_cluster - 1):
      print("find initial ......")
      p = self.data.mapPartitions(lambda x:find_closest_dist(x,centroids)).sortBy(lambda x:-x[1]).map(lambda x:x[0]).take(1)
      #print(p)
      centroids.append(p[0][1:])
    return centroids


    
  def fit(self):
    self.data.cache()
    self.n_dim = len(self.data.take(1)[0])-1

    diff = 100
    if self.n_cluster>16:
        cur_centroids = self.data.map(lambda x:x[1:]).takeSample(False,self.n_cluster)
    else:
        cur_centroids = self.initialPoint()
    #print("initial point: ",cur_centroids)
    count = 0
    while diff>0.1 and count<10:
      new_assign = self.data.map(lambda x:find_closest(x,cur_centroids))
      #print(new_assign.keys().distinct().collect())
      new_centroids = new_assign.mapValues(lambda x:x[1:]).aggregateByKey((0,[0]*self.n_dim),lambda x,y:(x[0]+1,addList(x[1],y)),lambda x,y:(x[0]+y[0],addList(x[1],y[1])))\
                      .sortBy(lambda x:x[0]).map(lambda x:[i/x[1][0] for i in x[1][1]]).collect()
      #print(cur_centroids[-3:])
      #print(new_centroids[-3:])
      print(len(new_centroids))
      if len(new_centroids) != self.n_cluster:
          cur_centroids = self.data.map(lambda x:x[1:]).takeSample(False,self.n_cluster)
          count = 0
          diff = 100
      else:
          diff = sum([math.sqrt(sum([(a-b)**2 for a,b in zip(new_centroids[l],cur_centroids[l])])) for l in range(self.n_cluster)])
          print(diff)
          cur_centroids = new_centroids
          count += 1

    return new_assign
  
  def fit_nonspark(self):
    diff = 100
    self.n_dim = len(self.data[0])-1
    cur_centroids = [i[1:] for i in random.sample(self.data,self.n_cluster)]
    #print(cur_centroids)
    count = 0
    while diff>0.1 and count<15:
      new_assign = []
      for i in self.data:
        new_assign.append(find_closest(i,cur_centroids))
      new_centroids = dict()
      for key,value in new_assign:
        if key not in new_centroids:
          new_centroids[key] = [0,[0]*self.n_dim]
        new_centroids[key][0]+=1
        new_centroids[key][1] = addList(new_centroids[key][1],value[1:])
      
      new_cen = [ [i/v[0] for i in v[1]] for k,v in sorted(new_centroids.items(),key = lambda x:x[0])]
      #print(new_cen)
      if len(new_cen) != self.n_cluster:
        cur_centroids = [i[1:] for i in random.sample(self.data,self.n_cluster)]
        count = 0
        diff = 100
      else:
        diff = sum([math.sqrt(sum([(a-b)**2 for a,b in zip(new_cen[l],cur_centroids[l])])) for l in range(self.n_cluster)])
        print(diff)
        cur_centroids = new_cen
        count+=1
    ans = {}
    for key,value in new_assign:
      if key not in ans:
        ans[key] = []
      ans[key].append(value)

    return ans



def cal_sigma(stat):
#[N,SUM,SUMSQ]
  temp = []
  for a,b in zip(stat[1],stat[2]):
    temp.append(math.sqrt(b/stat[0]-(a/stat[0])**2))
  return temp