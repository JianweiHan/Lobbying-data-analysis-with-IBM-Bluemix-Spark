
# coding: utf-8

# In[1]:

# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# sc is an existing SparkContext.
sqlContext = SQLContext(sc)


# In[2]:

jsonFile = sqlContext.read.json("swift://notebooks.spark/lobbying.json")


# In[3]:

print jsonFile


# In[4]:

jsonFile.registerTempTable("lobbyings");


# In[5]:

sqlContext.cacheTable("lobbyings")


# In[6]:

lobbyings = sqlContext.sql("SELECT * FROM lobbyings")


# In[7]:

lobbyings.cache()


# In[8]:

lobbyings.printSchema()


# In[9]:

from pyspark.sql.functions import explode


# In[10]:

clientState = sqlContext.sql("select lobbyings.PublicFilings.Filing.Client.`@ClientState` as state from lobbyings")


# In[11]:

clientState.printSchema()


# In[12]:

clientState.show()


# In[13]:

ClientState_flat = clientState.select(explode(clientState.state))


# In[14]:

ClientState_flat.show()


# In[15]:

ClientState_flatGroup = ClientState_flat.groupBy("_c0").count()


# In[16]:

ClientState_flat_sorted_df = ClientState_flatGroup.sort(["count"],ascending=False)


# In[17]:

ClientState_flat_sorted_df.show()


# In[18]:

ClientState_flat_sorted_df = ClientState_flat_sorted_df.filter(ClientState_flat_sorted_df['_c0'] != "")


# In[19]:

ClientState_flat_sorted_df.show()


# In[20]:

ClientState_flat_sorted_pandas = ClientState_flat_sorted_df.toPandas()
ClientState_flat_sorted_pandas


# In[21]:

get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

ind=np.arange(6)
width = 0.35
bar = plt.bar(ind, ClientState_flat_sorted_pandas["count"][0:6], width, color='b', label = "Lobbying numbers in the state")

params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches( (plSize[0]*2.5, plSize[1]*2) )
plt.ylabel('Lobbying numbers')
plt.xlabel('State in USA')
plt.title('Top 6 states for heavily lobbying')
plt.xticks(ind+width-0.1, ClientState_flat_sorted_pandas["_c0"][0:6])
plt.legend()

plt.show()


# In[22]:

issues = sqlContext.sql("select lobbyings.PublicFilings.Filing.Issues.Issue as issue from lobbyings")


# In[23]:

issues.show()


# In[24]:

issue_flat = issues.select(explode(issues.issue))


# In[25]:

issue_flat.show()


# In[29]:

issue_flat.printSchema()


# In[31]:

print issue_flat.count()


# In[33]:

issue_flat.registerTempTable("issue_flat");
sqlContext.cacheTable("issue_flat")
issue_flatNew = sqlContext.sql("SELECT * FROM issue_flat where _c0 not like '[%' and _c0 is not null")


# In[34]:

issue_flatNew.show()


# In[35]:

pandas = issue_flatNew.toPandas()


# In[36]:

for index, row in pandas.iterrows():
   row['_c0'] = row['_c0'].split(",")[0].split(":")[1]


# In[37]:

print pandas


# In[38]:

spark_issue = sqlContext.createDataFrame(pandas)


# In[39]:

spark_issue.printSchema()


# In[40]:

spark_issueGroup = spark_issue.groupBy('_c0').count()


# In[41]:

issue_sorted_df = spark_issueGroup.sort(["count"],ascending=False)


# In[42]:

issue_sorted_df.show()


# In[43]:

issue_sorted_pandas = issue_sorted_df.toPandas()


# In[45]:

get_ipython().magic(u'matplotlib inline')
import pandas as pd
import matplotlib.pyplot as plt

issue_sums = issue_sorted_pandas["count"][0:9]
other_sums = issue_sorted_pandas["count"][9:].sum()
issue_sums[9] = other_sums
issue_index = issue_sorted_pandas["_c0"][:9]
issue_index[9] = "OTHERS"

plt.axis('equal')
plt.title("Lobbying issue percentage",y=1.08)
plt.pie(
    issue_sums,
    labels=issue_index,
    colors=['blue', 'green', 'red', 'turquoise', 'magenta','yellow', "beige", 'gold', 'lightskyblue', 'lightcoral'],
    autopct="%1.2f%%",
    radius=1.25);


# In[ ]:



