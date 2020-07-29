import sys
from pyspark import SparkContext
import time
from itertools import islice
from graphframes import *
from itertools import islice
from pyspark.sql import SQLContext,SparkSession


if __name__ == '__main__':
    #<filter threshold> <input_file_path> <community_output_file_path>
    start = time.time()
    sc = SparkContext('local[*]', 'task')
    sc.setLogLevel("WARN")
    #ss = SparkSession.builder \
       # .master("local") \
       # .config("spark.some.config.option", "some-value") \
       # .getOrCreate()
    ss = SQLContext(sc)
    thres = int(sys.argv[1])    
    data = sc.textFile(sys.argv[2]).mapPartitionsWithIndex(lambda idx,row:islice(row,1,None) if idx ==0 else row).map(lambda x:x.split(','))

    b = data.map(lambda x:(x[1],x[0])).repartition(15).cache()
    edges = b.join(b).filter(lambda x:x[1][0]!=x[1][1]).map(lambda x:(x[1],x[0])).aggregateByKey([],lambda x,y:x+[y],lambda x,y:x+y).filter(lambda x:len(set(x[1]))>=thres).map(lambda x:x[0])
    sc.setCheckpointDir("s/")
    vertices = ss.createDataFrame(edges.flatMap(lambda x:list(x)).distinct().map(lambda x:[x]),["id"])
    g_edge = ss.createDataFrame(edges,["src","dst"])
    print(vertices.count())
    print(g_edge.count())
    g = GraphFrame(vertices,g_edge)

    ans = g.labelPropagation(maxIter=5).rdd.map(lambda x:(x[1],x[0])).aggregateByKey([],lambda x,y:x+[y],lambda x,y:x+y).map(lambda x:sorted(x[1])).sortBy(lambda x:(len(x),x[0])).collect()

    with open(sys.argv[3],"w") as file:
        for i in ans:
            file.write("'"+"', '".join(i)+"'"+'\n')
    end = time.time()
    print("Duration: ",end-start) 