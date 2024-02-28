#!/usr/bin/env python
# coding: utf-8

# ## process_bing_news
# 
# New notebook

# #### **Read the JSON file as a Dataframe**

# In[17]:


df = spark.read.option("multiline", "true").json("Files/bing-news.json")
# df now is a Spark DataFrame containing JSON data from "Files/bing-news.json".
display(df)


# #### **Selecting just the value column from the dataframe - include all JSON data we need**

# In[18]:


df = df.select("value")
display(df)


# #### **Explode the "value" column from the single row structure into multiple row structure**

# In[19]:


from pyspark.sql.functions import explode

df_exploded = df.select(explode(df["value"]).alias("json_object"))
display(df_exploded)


# #### **Converting the exploded JSON dataframe to a single JSON string list**

# In[20]:


json_list = df_exploded.toJSON().collect()

# to see the json structure of all the news articles from the list
print(json_list)

# to see the json structure of one news article from the list
#print(json_list[0])


# #### **To work with information its really easy to convert JSON string list to a JSON dictionary. This is how to do it using json.loads()**

# In[21]:


import json

article = json.loads(json_list[0])

print(article)


# #### **To get information from certain elements ex: "description"**

# In[22]:


print(article['json_object']['description'])


# #### **Lets get more information from the JSON Dictionary**

# In[23]:


# name
# description
# url
# image
# provider
# datePublished

# Can use :"Online JSON Parser" to see the structure of the JSON string - copy & paste the full string to the online tool.


# In[24]:


print(article['json_object']['name'])
print(article['json_object']['description'])
print(article['json_object']['url'])
print(article['json_object']['image']["thumbnail"]["contentUrl"])
print(article['json_object']['provider'][0]['name'])
print(article['json_object']['datePublished'])


# #### **Now, will loop through all the JSON dictionary lists and get the data**

# In[34]:


# Initialise empty lists
name = []
description = []
url = []
image = []
provider = []
datePublished = []

#process each JSON object in the list
for json_str in json_list:
    try:
        # Parse the JSON string into a dictionary
        article = json.loads(json_str)

        # This is optional - There might be data inconsistencies from the data comes from Bing API, in that case if some articles missing some data, we can write this if condition to check
        if article["json_object"].get("name") and article["json_object"].get("image", {}).get("thumbnail", {}).get("contentUrl"):

            # Extract information from the dictionary
            name.append(article["json_object"]["name"])
            description.append(article['json_object']['description'])
            url.append(article['json_object']['url'])
            image.append(article['json_object']['image']["thumbnail"]["contentUrl"])
            provider.append(article['json_object']['provider'][0]['name'])
            datePublished.append(article['json_object']['datePublished'])
    
    except Exception as e:
        print(f"Error processing JSON object: {e}")


# In[37]:


url


# #### **Combine all the lists together and create dataframe with a defined schema**

# In[38]:


from pyspark.sql.types import StructType, StructField, StringType

# Combine the lists
data = list(zip(name,description,url,image,provider,datePublished))

# Define Schema
schema = StructType([
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("url", StringType(), True),
        StructField("image", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("datePublished", StringType(), True)
])

# Create Dataframe
df_cleaned = spark.createDataFrame(data, schema=schema)


# In[39]:


display(df_cleaned)


# #### **Transform "datepublished" column data from timestamp to date data type**

# In[40]:


from pyspark.sql.functions import to_date, date_format

df_cleaned_final = df_cleaned.withColumn("datePublished", date_format(to_date("datePublished"), "dd-MM-yyyy"))


# In[42]:


display(df_cleaned_final)
# display(df_cleaned_final.limit(5))


# #### **Writing the final dataframe to the lakehouse db in Delta format**

# In[43]:


df_cleaned_final.write.format("delta").saveAsTable("bing_lake_db.tbl_latest_news")

