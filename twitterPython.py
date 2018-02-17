
# coding: utf-8

# In[1]:


import pixiedust
jarPath = "https://github.com/ibm-watson-data-lab/spark.samples/raw/master/dist/streaming-twitter-assembly-1.6.jar"
pixiedust.installPackage(jarPath)


# In[2]:


# twitter app credentials
ConsumerKey_twitter = ""
ConsumerSecret_twitter =""
AccessToken_twitter = ""
AccessTokenSecret_twitter = ""

#ibm watson bluemix credentials
Password_toneAnalyzer=""
UserName_toneAnalyzer=""


# In[5]:


get_ipython().run_cell_magic(u'scala', u'', u'\n// Starting authorization of twitter and ibm watson bluemix tone analyzer\nval auth = com.ibm.cds.spark.samples.StreamingTwitter\n\n// Initiating twitter credentials\nauth.setConfig("twitter4j.oauth.consumerKey",ConsumerKey_twitter)\nauth.setConfig("twitter4j.oauth.consumerSecret",ConsumerSecret_twitter)\nauth.setConfig("twitter4j.oauth.accessToken",AccessToken_twitter)\nauth.setConfig("twitter4j.oauth.accessTokenSecret",AccessTokenSecret_twitter)\n\n//Initiating ibm watson credentials\nauth.setConfig("watson.tone.url","https://gateway.watsonplatform.net/tone-analyzer/api")\nauth.setConfig("watson.tone.password",Password_toneAnalyzer)\nauth.setConfig("watson.tone.username",UserName_toneAnalyzer)\n\nimport org.apache.spark.streaming._\nauth.startTwitterStreaming(sc, Seconds(30))\n//if we do not specify time, then it will run untill it encounter stopTwitterStreaming')


# In[6]:


get_ipython().run_cell_magic(u'scala', u'', u'\n//Starting a new Twitter Stream that collects the live tweets and enrich them with Sentiment Analysis scores,\n//that is used as training data for the system. \n//The stream is run for a duration specified in the second argument of the startTwitterStreaming method,\n//as mentioned in the code aforementioned 30 seconds\n//Note: if no duration is specified then the stream will run until the stopTwitterStreaming method is called.\n    \nval auth = com.ibm.cds.spark.samples.StreamingTwitter\n\n// Creating sql_Context that will help to query the database which is hold in using data_frame\n//This function also creates a table named tweets from SparkSQL to get desired tweets at any given point of time\n\nval (__sqlContext, __df) = auth.createTwitterDataFrames(sc)')


# In[7]:


get_ipython().run_cell_magic(u'scala', u'', u'\nval auth = com.ibm.cds.spark.samples.StreamingTwitter\nval (__sqlContext, __df) = auth.createTwitterDataFrames(sc)\n//Quering sql_Context to retrieve all tweets stored using twitterStreaming service of SparkStreaming\nval comp =__sqlContext.sql("select * from tweets")\ncomp.show')


# In[8]:


get_ipython().run_cell_magic(u'scala', u'', u'val auth = com.ibm.cds.spark.samples.StreamingTwitter\nval (__sqlContext, __df) = auth.createTwitterDataFrames(sc)\n\n// Querying sql_Context for only those texts from tweets which have joy emotion value greater than 60% in the tone analyzer\nval joy = __sqlContext.sql("select text from tweets where Joy>60")\nprintln(joy.count)\njoy.show')


# In[9]:


get_ipython().run_cell_magic(u'scala', u'', u'val auth = com.ibm.cds.spark.samples.StreamingTwitter\nval (__sqlContext, __df) = auth.createTwitterDataFrames(sc)\n\n// Querying sql_Context for only those text,latitude and longitude value fromn tweets which have anger emotion value, \n//greater than 6% in the tone analyzer\nval tentative = __sqlContext.sql("select text,lat,long from tweets where Tentative>6")\nprintln(tentative.count)\ntentative.show')


# In[10]:


tweets=__df
tweets.count()
display(tweets)


# In[11]:


# Function to create an array named sentimentDistribution to store emotion value of each tweet gained from training data of tone Analyzer
# 13 is used as the data frame's last 13 data fields are emotions
sentiCount=[0] * 13

for i, sentiment in enumerate(tweets.columns[-13:]):
    sentiCount[i]=__sqlContext.sql("SELECT count(*) as sentCount FROM tweets where " + sentiment + " > 60")        .collect()[0].sentCount


# In[12]:


# Plotting tweets vs emotions, where the emotions value is greater than 40% using matplotlib and numpy library of python

get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

index=np.arange(13)
width = 0.30
bar = plt.bar(index, sentiCount, width, color='g', label = "distributions")

par = plt.gcf()
arr = par.get_size_inches()
par.set_size_inches( (arr[0]*5, arr[1]*2) )
plt.ylabel('Total count of tweets')
plt.xlabel('Sentiment Tone')
plt.title('Total number of tweets with sentiment value >60%')
plt.xticks(index+width, tweets.columns[-13:])
plt.legend()

plt.show()


# In[13]:


#Trying to find atmost top 10 trending tags, making use of regular expression libarary of python and using RDD of spark

from operator import add
import re
tags_RDD = tweets.flatMap( lambda t: re.split("\s", t.text))    .filter( lambda word: word.startswith("#") )    .map( lambda word : (word, 1 ))    .reduceByKey(add, 10).map(lambda (a,b): (b,a)).sortByKey(False).map(lambda (a,b):(b,a))
tentags = tags_RDD.take(10)

print(tentags)


# In[14]:


# Function to draw pie-chart to display atmost 10 trending tags

get_ipython().magic(u'matplotlib inline')
import matplotlib
import matplotlib.pyplot as plt

par = plt.gcf()
arr = par.get_size_inches()
par.set_size_inches( (arr[0]*2, arr[1]*2) )

labels = [i[0] for i in tentags]
sizes = [int(i[1]) for i in tentags]
colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral', "beige", "paleturquoise", "pink", "lightyellow", "coral"]

plt.pie(sizes, labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=90)

plt.axis('equal')
plt.show()


# In[15]:


col = tweets.columns[-13:]
def expand( t ):
    ret = []
    for s in [i[0] for i in tentags]:
        if ( s in t.text ):
            for tone in col:
                ret += [s.replace(':','').replace('-','') + u"-" + unicode(tone) + ":" + unicode(getattr(t, tone))]
    return ret 
def makeList(l):
    return l if isinstance(l, list) else [l]

tags_RDD = tweets.map(lambda t: t )

tags_RDD = tags_RDD.filter( lambda t: any(s in t.text for s in [i[0] for i in top10tags] ) )

tags_RDD = tags_RDD.flatMap( expand )

tags_RDD = tags_RDD.map( lambda fullTag : (fullTag.split(":")[0], float( fullTag.split(":")[1]) ))


tags_RDD = tags_RDD.combineByKey((lambda x: (x,1)),
                  (lambda x, y: (x[0] + y, x[1] + 1)),
                  (lambda x, y: (x[0] + y[0], x[1] + y[1])))

tags_RDD = tags_RDD.map(lambda (key, ab): (key.split("-")[0], (key.split("-")[1], round(ab[0]/ab[1], 2))))

tags_RDD = tags_RDD.reduceByKey( lambda x, y : makeList(x) + makeList(y) )

tags_RDD = tags_RDD.mapValues( lambda x : sorted(x) )

tags_RDD = tags_RDD.mapValues( lambda x : ([elt[0] for elt in x],[elt[1] for elt in x])  )

def customCompare( key ):
    for (k,v) in tentags:
        if k == key:
            return v
    return 0
tags_RDD = tags_RDD.sortByKey(ascending=False, numPartitions=None, keyfunc = customCompare)

tentagsMeanScores = tags_RDD.take(10)


# In[16]:


get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

par = plt.gcf()
arr = par.get_size_inches()
par.set_size_inches( (arr[0]*3, arr[1]*2) )

top5tagsMeanScores = tentagsMeanScores[:5]
width = 0
index=np.arange(13)
(a,b) = top5tagsMeanScores[0]
labels=b[0]
colors = ["beige", "paleturquoise", "pink", "lightyellow", "coral", "lightgreen", "gainsboro", "aquamarine","c"]
idx=0
for key, value in top5tagsMeanScores:
    plt.bar(ind + width, value[1], 0.15, color=colors[idx], label=key)
    width += 0.15
    idx += 1
plt.xticks(ind+0.3, labels)
plt.ylabel('AVERAGE SCORE')
plt.xlabel('TONES')
plt.title('Breakdown of top hashtags by sentiment tones')

plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='center',ncol=5, mode="expand", borderaxespad=0.)

plt.show()


# In[17]:


get_ipython().run_cell_magic(u'scala', u'', u'val demo = com.ibm.cds.spark.samples.PixiedustStreamingTwitter\ndemo.setConfig("twitter4j.oauth.consumerKey",ConsumerKey_twitter)\ndemo.setConfig("twitter4j.oauth.consumerSecret",ConsumerSecret_twitter)\ndemo.setConfig("twitter4j.oauth.accessToken",AccessToken_twitter)\ndemo.setConfig("twitter4j.oauth.accessTokenSecret",AccessTokenSecret_twitter)\ndemo.setConfig("watson.tone.url","https://gateway.watsonplatform.net/tone-analyzer/api")\ndemo.setConfig("watson.tone.password",Password_toneAnalyzer)\ndemo.setConfig("watson.tone.username",UserName_toneAnalyzer)\ndemo.setConfig("checkpointDir", System.getProperty("user.home") + "/pixiedust/ssc")')


# In[21]:


get_ipython().system(u'pip install --user pixiedust_twitterdemo')


# In[22]:


from pixiedust_twitterdemo import *
twitterDemo()


# In[21]:


display(__tweets)


# In[22]:


from pyspark.sql import Row
from pyspark.sql.types import *
emotions=__tweets.columns[-13:]
distrib = __tweets.flatMap(lambda t: [(x,t[x]) for x in emotions]).filter(lambda t: t[1]>60)    .toDF(StructType([StructField('emotion',StringType()),StructField('score',DoubleType())]))
display(distrib)


# In[23]:


__tweets.registerTempTable("pixiedust_tweets")

sentiCount=[0] * 13

for i, sentiment in enumerate(__tweets.columns[-13:]):
    sentiCount[i]=sqlContext.sql("SELECT count(*) as sentCount FROM pixiedust_tweets where " + sentiment + " > 60")        .collect()[0].sentCount


# In[25]:


get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

index=np.arange(13)
width = 0.35
bar = plt.bar(ind, sentimentDistribution, width, color='g', label = "distributions")

par = plt.gcf()
arr = par.get_size_inches()
par.set_size_inches( (arr[0]*5, arr[1]*2) )
plt.ylabel('Tweet count')
plt.xlabel('Tone')
plt.title('Distribution of tweets by sentiments > 60%')
plt.xticks(index+width, __tweets.columns[-13:])
plt.legend()

plt.show()


# In[26]:


from operator import add
import re
tags_RDD = __tweets.flatMap( lambda t: re.split("\s", t.text))    .filter( lambda word: word.startswith("#") )    .map( lambda word : (word, 1 ))    .reduceByKey(add, 10).map(lambda (a,b): (b,a)).sortByKey(False).map(lambda (a,b):(b,a))
tentags = tags_RDD.take(10)


# In[27]:


get_ipython().magic(u'matplotlib inline')
import matplotlib
import matplotlib.pyplot as plt

par = plt.gcf()
arr = par.get_size_inches()
par.set_size_inches( (arr[0]*2, arr[1]*2) )

labels = [i[0] for i in tentags]
sizes = [int(i[1]) for i in tentags]
colors = ['yellowgreen', 'gold', 'lightskyblue', 'lightcoral', "beige", "paleturquoise", "pink", "lightyellow", "coral"]

plt.pie(sizes, labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=90)

plt.axis('equal')
plt.show()


# In[28]:


cols = __tweets.columns[-13:]
def expand( t ):
    ret = []
    for s in [i[0] for i in tentags]:
        if ( s in t.text ):
            for tone in col:
                ret += [s.replace(':','').replace('-','') + u"-" + unicode(tone) + ":" + unicode(getattr(t, tone))]
    return ret 
def makeList(l):
    return l if isinstance(l, list) else [l]

tags_RDD = tweets.map(lambda t: t )

tags_RDD = tags_RDD.filter( lambda t: any(s in t.text for s in [i[0] for i in top10tags] ) )

tags_RDD = tags_RDD.flatMap( expand )

tags_RDD = tags_RDD.map( lambda fullTag : (fullTag.split(":")[0], float( fullTag.split(":")[1]) ))


tags_RDD = tags_RDD.combineByKey((lambda x: (x,1)),
                  (lambda x, y: (x[0] + y, x[1] + 1)),
                  (lambda x, y: (x[0] + y[0], x[1] + y[1])))

tags_RDD = tags_RDD.map(lambda (key, ab): (key.split("-")[0], (key.split("-")[1], round(ab[0]/ab[1], 2))))

tags_RDD = tags_RDD.reduceByKey( lambda x, y : makeList(x) + makeList(y) )

tags_RDD = tags_RDD.mapValues( lambda x : sorted(x) )

tags_RDD = tags_RDD.mapValues( lambda x : ([elt[0] for elt in x],[elt[1] for elt in x])  )

def customCompare( key ):
    for (k,v) in tentags:
        if k == key:
            return v
    return 0
tags_RDD = tags_RDD.sortByKey(ascending=False, numPartitions=None, keyfunc = customCompare)

tentagsMeanScores = tags_RDD.take(10)


# In[29]:


get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

par = plt.gcf()
arr = par.get_size_inches()
par.set_size_inches( (arr[0]*3, arr[1]*2) )

top5tagsMeanScores = tentagsMeanScores[:5]
width = 0
ind=np.arange(13)
(a,b) = top5tagsMeanScores[0]
labels=b[0]
colors = ["beige", "paleturquoise", "pink", "lightyellow", "coral", "lightgreen", "gainsboro", "aquamarine","c"]
idx=0
for key, value in top5tagsMeanScores:
    plt.bar(ind + width, value[1], 0.15, color=colors[idx], label=key)
    width += 0.15
    idx += 1
plt.xticks(ind+0.3, labels)
plt.ylabel('AVERAGE SCORE')
plt.xlabel('TONES')
plt.title('Breakdown of top hashtags by sentiment tones')

plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='center',ncol=5, mode="expand", borderaxespad=0.)

plt.show()

