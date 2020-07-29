import sys
import json
from pyspark import SparkContext


if __name__ == '__main__':
  sc = SparkContext.getOrCreate()
  
  review = sc.textFile(sys.argv[1])
  business = sc.textFile(sys.argv[2])
  filter_bus = sys.argv[3]
  bus = business.map(lambda x:json.loads(x)).map(lambda x:(x["business_id"],x["state"])).filter(lambda x:x[1] == filter_bus).distinct()
  header = sc.parallelize([("user_id","business_id")])
  header.union(review.map(lambda x:json.loads(x)).map(lambda x:(x["business_id"],x["user_id"])).join(bus).map(lambda x:(x[1][0],x[0]))).repartition(1).map(lambda x:",".join(x)).saveAsTextFile("output_folder")