import sys
import json
import re
import tweepy
import random
from pyspark import SparkContext
from time import gmtime, strftime

class MyStreamListener(tweepy.StreamListener):
  def __init__(self,api = None):
    self.api = api
    self.seq_Num = 0
    self.memory = []
    self.count ={}
    self.capacity = 100
    self.out = sys.argv[2]

  def output(self):
    sort_large = sorted(set(self.count.values()),reverse=True)
    third_large = sort_large[min(len(sort_large)-1,2)]
    ans = sorted([(k,v) for k,v in self.count.items() if v >=max(third_large,1)],key = lambda x:(-x[1],x[0]))
    with open(self.out,"a") as file:
      file.write("The number of tweets with tags from the beginning: {}\n".format(self.seq_Num))
      for k,v in ans:
        file.write('{}: {}\n'.format(k,v))
      file.write('\n')
    '''
    print("The number of tweets with tags from the beginning: {}".format(self.seq_Num))
    for k,v in ans:
        print('{}: {}'.format(k,v))
    print('')
    '''
  
  def extract_tags(self,lists):
    temp = []
    for l in lists:
      if l['text'].encode('UTF-8').isalpha():
          temp.append(l['text'])
          if l['text'] not in self.count:
            self.count[l['text']] = 0
            #print(l['text'])
          self.count[l['text']] += 1
    return temp

  def on_status(self, status):
    if status.entities["hashtags"]:
      self.seq_Num += 1   

      if self.seq_Num<=self.capacity:
        temp = self.extract_tags(status.entities['hashtags'])
        if len(temp)>0:
            self.memory.append(temp)
            self.output()           
        else:
            self.seq_Num -= 1
            #print("no utf8")            
      else:
        prob = self.capacity/self.seq_Num
        p = random.choices([0,1],[1-prob,prob])[0]
        if p == 1:
          pick = random.randint(0,self.capacity-1)
          remove = self.memory[pick]
          self.memory[pick] = self.extract_tags(status.entities['hashtags'])
          for i in remove:
            self.count[i]-=1
        self.output()

      #print(status.entities["hashtags"])


if __name__ == "__main__":
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("ERROR")
  api_key='srr6jIKTKrtL7xMCiDOQALzgg'
  api_secret = 'qbS1Jv4lVnsGSksNLxlERbQYUKusEu14y1cBd4VJKRcWaMGk4L'
  access_token='957962013090578433-K2adDuMl8PHpyCiMbYmnHJ4HVdIDHiW'
  access_token_secret = '109Wm0ncFBnvwk8gLVwGDin3UVE1Of8jbfcy1e5ZtSS13'

  auth = tweepy.OAuthHandler(api_key, api_secret)
  auth.set_access_token(access_token, access_token_secret)
  with open(sys.argv[2],"w") as file:
    file.write("")
  api = tweepy.API(auth)
  myStreamListener = MyStreamListener()
  myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
  myStream.sample()
    