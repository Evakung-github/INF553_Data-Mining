import sys
import json
import re
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

if __name__ == '__main__':
    spark = True
    if sys.argv[4] == "spark":   
        review = sc.textFile(sys.argv[1])
        business = sc.textFile(sys.argv[2])
    else:
        spark = False
        with open(sys.argv[1],"r") as file:
            review = list(map(lambda x: json.loads(x),file.readlines()))
        with open(sys.argv[2],"r") as file:
            business= list(map(lambda x: json.loads(x),file.readlines()))
 

    output = sys.argv[3]
    stopwords = []
    n = int(sys.argv[5])
    ans = dict()
    
    if spark:
        b = business.map(lambda x:json.loads(x)).filter(lambda x:x['categories'] != None).map(lambda x:(x['business_id'],[i.strip() for i in x['categories'].split(',')]))
        r = review.map(lambda x:json.loads(x)).map(lambda x:(x["business_id"],x["stars"]))
        ans['result'] = b.join(r).flatMap(lambda x:[(i,x[1][1]) for i in x[1][0]]).aggregateByKey((0,0),lambda x,y:(x[0]+y,x[1]+1),lambda x,y:(x[0]+y[0],x[1]+y[1])).mapValues(lambda x:float(x[0]/x[1])).sortBy(lambda x:(-x[1],x[0])).take(n)
    else:

        review_map = {}

        for i in review:
            if i["business_id"] in review_map:
                review_map[i["business_id"]][0]+= i["stars"]
                review_map[i["business_id"]][1]+= 1
            else:
                review_map[i["business_id"]]=[i["stars"],1]
        
        category_map = {}
        for i in business:
            if i["business_id"] not in review_map or i['categories']==None:
                #print(i["business_id"]," not in review")
                continue
            for j in i['categories'].split(','):
                j = j.strip()
                if j not in category_map:
                    category_map[j] = [0,0]
                category_map[j][0] += review_map[i['business_id']][0]
                category_map[j][1] += review_map[i['business_id']][1]
        #print(category_map)
        for i in category_map:
            category_map[i] = category_map[i][0]/category_map[i][1]
        #print(category_map)

        ans['result'] = [(k,v) for k,v in sorted(category_map.items(),key = lambda x:(-x[1],x[0]))[:n]] 
    with open(output,"w") as file:
        file.write(json.dumps(ans,indent=4))

