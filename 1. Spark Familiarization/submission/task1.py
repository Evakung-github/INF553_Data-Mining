import sys
import json
import re
from pyspark import SparkContext
sc = SparkContext.getOrCreate()


if __name__ == '__main__':
    t = sc.textFile(sys.argv[1])
    output = sys.argv[2]
    stopwords = set()
    with open(sys.argv[3]) as file:
        for i in file:
            stopwords.add(i.split()[0])
    stopwords.add("")
    
    y,m,n = sys.argv[4],sys.argv[5],sys.argv[6]
    ans = dict()
    
    
    ans['A'] = t.count()
    ans['B'] = t.filter(lambda x:json.loads(x)['date'][:4]==y).count()
    ans['C'] = t.map(lambda x:json.loads(x)['user_id']).distinct().count()
    ans['D'] = t.map(lambda x:(json.loads(x)["user_id"],1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:(-x[1],x[0])).take(int(m))
    ans['E'] = t.map(lambda x:json.loads(x)['text']).flatMap(lambda x:re.split("[ \\(\\[,\n.!?:;\\]\\)]+",x.lower())).filter(lambda x:x.strip() not in stopwords).map(lambda x:(x.strip(),1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:(-x[1],x[0])).map(lambda x:x[0]).take(int(n))

#.map(lambda x:x[0]).
    with open(output,"w") as file:
        file.write(json.dumps(ans,indent=4))


