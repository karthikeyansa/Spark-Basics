#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark


# In[3]:


findspark.init('/home/ky/spark-3.0.1-bin-hadoop3.2')


# # Just in case to find pysparklib

# In[4]:


from pyspark.sql import SparkSession


# In[5]:


spark = SparkSession.builder.appName('Stocks').getOrCreate()


# In[45]:


df = spark.read.csv('dataframe/WMT.csv', header= True, inferSchema= True)


# In[46]:


df.printSchema()


# In[47]:


df.show()


# ## To get Colums names

# In[48]:


df.columns


# ## To get first 5 rows

# In[49]:


df.limit(5).collect()


# In[50]:


df.describe().show()


# In[51]:


from pyspark.sql.functions import format_number


# In[52]:


## To get two decimal val from mean, stddev(standard deviation)


# In[53]:


result = df.describe()


# In[55]:


result.select(result['summary'],format_number(result['Open'].cast('float'), 2).alias('Open')).show()


# In[60]:


# a result to get HV ratio means result of high and volume ratio 


# In[62]:


df2 = df.withColumn('HV Ratio', df['High']/df['Volume'])


# In[74]:


df2.select(df2['HV Ratio']).show()


# In[75]:


# get date with peak high in price


# In[78]:


df.orderBy(df['High'].desc()).head(1)[0][0]


# In[96]:


from pyspark.sql.functions import mean, max, min


# In[97]:


df.select(mean('Close')).show()


# In[99]:


df.select(min('Close'), max('Close')).show()


# In[103]:


## days was the close lower than 200 using filter


# In[112]:


df.filter('Close < 200').count()


# In[114]:


df.select(df['Close'] < 200).count()


# In[116]:


df.select(df['High'] > 80).count()/df.count()


# In[151]:


from pyspark.sql.functions import corr, year, avg, month


# In[152]:


df.select(corr('High', 'Volume')).show()


# In[153]:


yeardf = df.withColumn('Year', year(df['Date']))


# In[164]:


yeardf.groupBy('Year').max('High').show()


# In[169]:


monthdf = df.withColumn('Month', month(df['Date']))


# In[172]:


monthdf.groupBy('Month').avg('Close').show()


# In[ ]:




