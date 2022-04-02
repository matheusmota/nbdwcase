import os, socket, sys, logging
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
import json
from .utils import describe_dataframe, add_prefix_to_df_columns, validate_json, anonimize_columns, compare_dfs



# def build_order_dataset(sc):

#     sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
#     sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
#     sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://{}:9000".format(socket.gethostbyname_ex('minio-s3')[2][0]))
#     sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
#     sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

#     sqlc = pyspark.sql.SQLContext(sc)



#     # In[3]:


#     DEBUG=False


#     # In[4]:




#     # ## Reading main dataframes

#     # ### Restaurant

#     # In[6]:


#     df_restaurant = sqlc.read.parquet('s3a://raw-data/restaurant.csv.parquet')


#     # In[7]:


#     if(DEBUG):
#         df_restaurant.printSchema()


#     # ### Order Status

#     # In[8]:


#     df_status = sqlc.read.parquet('s3a://raw-data/status.json.parquet')


#     # In[9]:


#     if(DEBUG):
#         df_status.printSchema()


#     # ### Consumer

#     # In[10]:


#     df_consumer = sqlc.read.csv('s3a://raw-data/consumer.csv', header='true', inferSchema='true')


#     # In[11]:


#     if(DEBUG):
#         df_consumer.printSchema()


#     # ### Order

#     # In[12]:


#     df_order = sqlc.read.parquet('s3a://raw-data/order.json.parquet')


#     # In[13]:


#     if(DEBUG):
#         df_order.printSchema()


#     # ## Dataframe Sanitization & Preparation

#     # More work on this matter should be investigated. We would probaly need more generic solutions on this problem; maybe an internal tool, specially considering business scenarios. In this solution only critical columns will be addressed.

#     # In[14]:


#     import pyspark.sql.functions as F
#     import pyspark.sql.types as T


#     # ### Status Dataframe

#     # #### Handling schema-related issues: fixing timestamp columns

#     # In[15]:


#     df_status = df_status.withColumn('created_at',F.unix_timestamp(F.lit(df_status.created_at),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast("timestamp"))


#     # In[16]:


#     if(DEBUG):
#         df_status.printSchema()


#     # #### Handling duplicates

#     # In[17]:


#     if(DEBUG):
#         describe_dataframe(df_status)


#     # In[18]:


#     df_status = df_status.dropDuplicates()


#     # In[19]:


#     if(DEBUG):
#         describe_dataframe(df_status)


#     # In[20]:


#     if(DEBUG):
#         display(df_status.select("value").distinct())


#     # In[21]:


#     if(DEBUG):
#         print(df_status.groupBy("order_id").count().filter("count != 3").count())
#         df_status.groupBy("order_id").count().filter("count != 3").sort(F.desc("count")).show(3)


#     # It seems that the same status appears multiple times. Checking status counting.

#     # In[22]:


#     if(DEBUG):
#         df_status.groupBy("order_id", "value").count().filter("count > 1 AND value == 'REGISTERED' ").sort(F.desc("count")).show(5, False)
#         df_status.groupBy("order_id", "value").count().filter("count > 1 AND value == 'CONCLUDED' ").sort(F.desc("count")).show(10, False)
#         df_status.groupBy("order_id", "value").count().filter("count > 1 AND value == 'PLACED' ").sort(F.desc("count")).show(5, False)


#     # In[23]:


#     if(DEBUG):
#         df_status.filter("order_id == '1207262a-e90d-40eb-8714-080490acc201' ").sort("created_at").show(5,False)


#     # In[24]:


#     df_status_agg_by_order_and_value =  df_status.groupBy("order_id","value").agg(F.count("value").alias("count"), F.collect_list("created_at").alias("created_at_array"))                                             .withColumn("min_created_at", F.coalesce(F.array_min("created_at_array")))                                             .withColumn("max_created_at", F.coalesce(F.array_max("created_at_array")))                                             .withColumn("diff_created_at", F.unix_timestamp("max_created_at") - F.unix_timestamp("min_created_at"))


#     # In[25]:


#     if(DEBUG):
#         df_status_agg_by_order_and_value.filter("diff_created_at > 0").orderBy(*["order_id"], ascending=False).show(10, False)
#         df_status_agg_by_order_and_value.groupBy("order_id").agg(F.count("order_id").alias("count")).filter("count !=3").sort(F.desc("count")).show(5,False)


#     # In[26]:


#     if(DEBUG):
#         df_status.filter("order_id == 'adefb27b-7ea7-46a8-b0ef-41f2f868b0b9'").sort("created_at").show(5,False)
#         df_status.filter("order_id == 'a92c7759-5f08-419b-877c-d75842481e69'").sort("created_at").show(5,False)    


#     # Ok. I will have to check if it is a parsing problem.

#     # In[27]:


#     if(DEBUG):
#         sqlc.read.json('s3a://raw-data/status.json').filter(" order_id == '01d83b11-450a-4917-b2d8-2121b1f3f5ef'").sort("created_at").show(10,False)


#     # Odd. No business information available, so I have to make some decisions. 
#     # 
#     # I decided to consider the max(timestamp). Requeriments indicate LAST as more useful information.
#     # 
#     # Just in case, lets check if all orders have at least one "PLACED" status.

#     # In[28]:


#     if(DEBUG):
#         print(df_status.select("order_id").distinct().count())
#         print(df_status.select("order_id", "value").filter("value == 'PLACED'").distinct().count())
#         print(df_status.select("order_id", "value").filter("value == 'REGISTERED'").distinct().count())    


#     # Nope.

#     # #### Business-related transformations (requirements)
#     # ##### Calculating the last status for each order
#     # 
#     # Requirement: "add the the LAST status from order statuses dataset"
#     # 

#     # In[29]:


#     df_status_agg_by_order = df_status.sort("created_at").groupBy("order_id").agg(F.count("value").alias("count_value"), F.collect_list("created_at").alias("created_at_array"), F.collect_list("value").alias("value_array"), )                                  .withColumn("first_created_at", F.coalesce(F.array_min("created_at_array")))                                  .withColumn("last_created_at",  F.coalesce(F.array_max("created_at_array")))                                  .withColumn("diff_last_and_first_created_at", F.unix_timestamp("last_created_at") - F.unix_timestamp("first_created_at"))


#     # In[30]:


#     if(DEBUG):
#         df_status_agg_by_order.filter("diff_last_and_first_created_at > 0 AND count_value != 3").orderBy(*["order_id"], ascending=False).show(5, False)


#     # In[31]:


#     df_status_agg_by_order = df_status_agg_by_order.withColumnRenamed("last_created_at","created_at").join(df_status,['order_id','created_at']).withColumnRenamed("created_at","last_created_at").withColumnRenamed("value","last_value").withColumnRenamed("status_id","last_status_id")


#     # In[32]:


#     if(DEBUG):
#         df_status_agg_by_order.show(3, False)


#     # In[33]:


#     if(DEBUG):
#         describe_dataframe(df_status_agg_by_order.select("order_id"))


#     # ### Order Dataframe

#     # #### Handling schema-related issues: fixing timestamp columns

#     # In[34]:


#     df_order = df_order.withColumn('order_created_at',F.unix_timestamp(F.lit(df_order.order_created_at),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast("timestamp"))


#     # In[35]:


#     if(DEBUG):
#         df_order.printSchema()


#     # #### Handling schema-related issues: JSON parsing problems
#     # 
#     # 
#     # A User Defined Function was built to address formatting problems. The UDF was moved to the utils package.

#     # In[36]:

#     parse_and_fix_json = F.udf(validate_json, T.StringType())

#     df_order = df_order.withColumn("items_json_text", parse_and_fix_json(F.col("items")))

#     invalid_json_items_count = df_order.filter(df_order.items_json_text == 'invalid').count()

#     if(DEBUG):
#         print("Total of invalid json items (df_order): {}".format(invalid_json_items_count))


#     # Infering the schema for *items*:

#     # In[37]:


#     items_json_schema = sqlc.read.json(df_order.rdd.map(lambda row: row.items_json_text)).schema


#     # In[38]:


#     df_order = df_order.withColumn('items', F.from_json(F.col("items_json_text"), items_json_schema)).drop("items_json_text")


#     # In[39]:


#     df_order = df_order.withColumn("items", F.col("items").data)


#     # Remark about previous schema building: we could improve this infering process by taking a sample of data. Further evaluation/heuristics of a proper strategy will be needed and therefore will not be addressed here.
#     # 

#     # In[40]:


#     if(DEBUG):
#         df_order.printSchema()


#     # Internal elements of `items` would need to be better 'typed' (e.g., there are some fields being treated as string but should be correctly 'typed' as numbers).

#     # Due to time restriction, other columns are being neglected here.

#     # #### Anonymizing sensitive data

#     # In[41]:


#     # To Do: move to https://pypi.org/project/spark-privacy-preserver/

#     # Further information can be anonimized here
#     sensitive_columns = ["cpf", "customer_name", "delivery_address_latitude", "delivery_address_longitude", "delivery_address_zip_code"]
#     df_order = anonimize_columns(df_order, target_columns=sensitive_columns, numBits=256)


#     # #### Handling duplicates

#     # In[42]:


#     if(DEBUG):
#         describe_dataframe(df_order)


#     # In[43]:


#     if(DEBUG):
#         describe_dataframe(df_order.select("order_id"))


#     # Ok, we have a problem. Something is duplicated. I took a sample and noticed `order_created_at` and `cpf` causing duplicates.
#     # Confirming:

#     # In[44]:


#     if(DEBUG):
#         describe_dataframe(df_order.drop("cpf").dropDuplicates())


#     # In[45]:


#     if(DEBUG):
#         describe_dataframe(df_order.drop("order_created_at").dropDuplicates())


#     # In[46]:


#     if(DEBUG):
#         describe_dataframe(df_order.drop(*["cpf","order_created_at"]).dropDuplicates())


#     # In[47]:


#     if(DEBUG):
#         describe_dataframe(df_order.drop(*["cpf","order_created_at"]).dropDuplicates().select("order_id"))


#     # Ok, lets build the final df for orders without losing the information causing duplicated orders:

#     # In[ ]:


#     df_order_agg_by_order_id = df_order.groupBy("order_id").agg(F.collect_list("order_created_at").alias("order_created_at_array"), F.collect_list("cpf").alias("cpf_array"))


#     # In[ ]:


#     if(DEBUG):
#         display(df_order_agg_by_order_id.limit(3))
#         describe_dataframe(df_order_agg_by_order_id.select("order_id"))    


#     # Merging will all other columns:

#     # In[ ]:


#     df_order_agg_by_order_id = df_order_agg_by_order_id.join(df_order.drop(*["cpf","order_created_at"]).dropDuplicates(), "order_id")


#     # In[ ]:


#     if(DEBUG):
#         display(df_order_agg_by_order_id.limit(3))
#         describe_dataframe(df_order_agg_by_order_id.select("order_id"))    
#         df_order_agg_by_order_id.printSchema()


#     # Removing duplicated columns

#     # In[ ]:


#     df_order_agg_by_order_id = df_order_agg_by_order_id.drop(*["customer_name"])


#     # ### Consumer Dataframe

#     # #### Anonymizing sensitive data

#     # In[ ]:


#     # To Do: move to https://pypi.org/project/spark-privacy-preserver/

#     # Further information can be anonimized here
#     sensitive_columns = ["customer_name", "customer_phone_number"]
#     df_consumer = anonimize_columns(df_consumer, target_columns=sensitive_columns, numBits=256)


#     # ## Building trusted dataset (requirements)

#     # It seems that we are ready to meet all the requirements for the datamart.
#     # 
#     # First, lets **avoid column key crashes**:

#     # In[ ]:


#     if(DEBUG):
#         df_restaurant.printSchema()

#     df_restaurant = add_prefix_to_df_columns(df_restaurant,"merchant", escape=[])

#     if(DEBUG):
#         df_restaurant.printSchema()


#     # In[ ]:


#     if(DEBUG):
#         df_consumer.printSchema()

#     df_consumer   = add_prefix_to_df_columns(df_consumer,"customer", escape=[])

#     if(DEBUG):
#         df_consumer.printSchema()


#     # In[ ]:


#     if(DEBUG):
#         df_status_agg_by_order.printSchema()

#     df_status_agg_by_order = add_prefix_to_df_columns(df_status_agg_by_order,"status", escape=['order_id'])  

#     if(DEBUG):
#         df_status_agg_by_order.printSchema()


#     # ### Order Dataset

#     # #### Joining order with consumer, restaurant, and status
#     # 
#     # Requirement: *one line per order with all data from order, consumer, restaurant*

#     # In[ ]:


#     df_trusted_order = df_order_agg_by_order_id.join(df_consumer, 'customer_id', "left_outer")


#     # In[ ]:


#     df_trusted_order = df_trusted_order.join(df_restaurant, 'merchant_id', "left_outer")


#     # In[ ]:


#     df_trusted_order = df_trusted_order.join(df_status_agg_by_order, 'order_id', "left_outer")


#     # In[ ]:


#     if(DEBUG):
#         display(df_trusted_order.limit(3))
#         describe_dataframe(df_trusted_order.select("order_id"))    


#     # #### Adding LOCAL datetime (will be used for partitioning, as required)
#     # 
#     # Requirement: *To help analysis, it would be a nice to have: data partitioned on the restaurant LOCAL date.*

#     # In[ ]:


#     df_trusted_order = df_trusted_order.withColumn("localtime_order_created_at",pyspark.sql.functions.to_utc_timestamp(F.col("status_last_created_at"), F.col("merchant_timezone")))


#     # In[ ]:


#     df_trusted_order = df_trusted_order.withColumn("localtime_order_created_at_date", F.to_date(F.col("localtime_order_created_at") ))


#     # #### Metrics and validation
#     # 
#     # As a simple validation, I will check if all ids (customers, merchants, orders, order_id from df_status are in the same final dataframe). I validate this aspect by a simple count.

#     # In[ ]:


#     # df_trusted_order = df_order_agg_by_order_id.join(df_consumer, 'customer_id')

#     key_dfs=[df_trusted_order.select("customer_id").where(F.col("customer_id").isNotNull()), 
#              df_consumer.select("customer_id")
#     ]
#     compare_dfs(key_dfs)


#     # In[ ]:


#     #df_trusted_order = df_trusted_order.join(df_restaurant, 'merchant_id')

#     key_dfs=[df_trusted_order.select("merchant_id").where(F.col("merchant_id").isNotNull()), 
#              df_restaurant.select("merchant_id")
#     ]
#     compare_dfs(key_dfs)


#     # In[ ]:


#     # df_trusted_order = df_trusted_order.join(df_order, 'order_id')

#     key_dfs=[df_trusted_order.select("order_id").where(F.col("order_id").isNotNull()), 
#              df_order.select("order_id")
#     ]
#     compare_dfs(key_dfs)


#     # #### Creating datamart - Storing order-dataset in trusted layer partioned by LOCAL date

#     # In[ ]:


#     df_trusted_order.repartition("localtime_order_created_at_date").write.partitionBy("localtime_order_created_at_date").mode("overwrite").format("parquet").save("s3a://trusted-data/order-dataset.parquet")


#     # ### Order Items Dataset 

#     # #### Exploding items externalIds and linking to order_ids

#     # In[ ]:


#     df_orders_trusted_layer = sqlc.read.parquet("s3a://trusted-data/order-dataset.parquet")


#     # In[ ]:


#     df_trusted_order_items = df_orders_trusted_layer.select('order_id','items').withColumn('items', F.explode('items'))


#     # In[ ]:


#     if(DEBUG):
#         describe_dataframe(df_trusted_order_items)


#     # In[ ]:


#     df_trusted_order_items = df_trusted_order_items.select("order_id", "items.name", "items.externalId")


#     # In[ ]:


#     if(DEBUG):
#         df_trusted_order_items.show(10, True)


#     # In[ ]:


#     df_trusted_order_items = df_trusted_order_items.groupBy("externalId").agg(F.collect_list("order_id").alias("order_ids"), F.collect_set("name").alias("item_names"))


#     # #### Creating datamart - Storing order_items dataset in trusted layer

#     # In[ ]:


#     df_trusted_order_items.repartition(1).write.format("parquet").mode("append").save("s3a://trusted-data/order_items-dataset.parquet")


#     # ### Order Statuses Dataset

#     # In[ ]:


#     df_orders_trusted_layer = sqlc.read.parquet("s3a://trusted-data/order-dataset.parquet")


#     # In[ ]:


#     df_status_raw = sqlc.read.parquet('s3a://raw-data/status.json.parquet')


#     # #### Pivoting on values

#     # In[ ]:


#     df_status_raw = df_status_raw.groupBy('order_id').pivot("value").agg(F.collect_set("created_at"))


#     # In[ ]:


#     if(DEBUG):
#         df_status_raw.printSchema()


#     # In[ ]:


#     if(DEBUG):
#         describe_dataframe(df_orders_trusted_layer.select("order_id"))
#         describe_dataframe(df_status_raw.select("order_id"))



#     # Yep. We should merge with order dataset to make sure all ids are there

#     # #### Merging with missing ids

#     # In[ ]:


#     df_status_raw = df_status_raw.join(df_orders_trusted_layer.select("order_id"), 'order_id', "right_outer")


#     # In[ ]:


#     if(DEBUG):
#         describe_dataframe(df_orders_trusted_layer.select("order_id"))
#         describe_dataframe(df_status_raw.select("order_id"))



#     # In[ ]:


#     df_status_raw.show(5)


#     # #### Creating datamart - Storing order_items dataset in trusted layer

#     # In[ ]:


#     df_status_raw.repartition(1).write.format("parquet").mode("append").save("s3a://trusted-data/order_statuses-dataset.parquet")



    
    
    
    
    
    
    

def build_order_dataset(sc, log_level=logging.INFO):

    logging.basicConfig(level = log_level)
    
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://{}:9000".format(socket.gethostbyname_ex('minio-s3')[2][0]))
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    sqlc = pyspark.sql.SQLContext(sc)

    

    logging.info("Reading main dataframes")

    logging.info("Reading restaurant dataframe")
    df_restaurant = sqlc.read.parquet('s3a://raw-data/restaurant.csv.parquet')


    logging.info("Reading order status dataframe")
    df_status = sqlc.read.parquet('s3a://raw-data/status.json.parquet')


    logging.info("Reading consumer dataframe")
    df_consumer = sqlc.read.csv('s3a://raw-data/consumer.csv', header='true', inferSchema='true')


    logging.info("Reading order dataframe")
    df_order = sqlc.read.parquet('s3a://raw-data/order.json.parquet')



    logging.info("Dataframes Sanitization / Preparation")
    logging.info("Preparing status dataframe")

    df_status = df_status.withColumn('created_at',F.unix_timestamp(F.lit(df_status.created_at),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast("timestamp"))

    # Handling duplicates

    df_status = df_status.dropDuplicates()  

    df_status_agg_by_order_and_value =  df_status.groupBy("order_id","value").agg(F.count("value").alias("count"), F.collect_list("created_at").alias("created_at_array"))                                             .withColumn("min_created_at", F.coalesce(F.array_min("created_at_array")))                                             .withColumn("max_created_at", F.coalesce(F.array_max("created_at_array")))                                             .withColumn("diff_created_at", F.unix_timestamp("max_created_at") - F.unix_timestamp("min_created_at"))


    df_status_agg_by_order = df_status.sort("created_at").groupBy("order_id").agg(F.count("value").alias("count_value"), F.collect_list("created_at").alias("created_at_array"), F.collect_list("value").alias("value_array"), )                                  .withColumn("first_created_at", F.coalesce(F.array_min("created_at_array")))                                  .withColumn("last_created_at",  F.coalesce(F.array_max("created_at_array")))                                  .withColumn("diff_last_and_first_created_at", F.unix_timestamp("last_created_at") - F.unix_timestamp("first_created_at"))



    df_status_agg_by_order = df_status_agg_by_order.withColumnRenamed("last_created_at","created_at").join(df_status,['order_id','created_at']).withColumnRenamed("created_at","last_created_at").withColumnRenamed("value","last_value").withColumnRenamed("status_id","last_status_id")


    logging.info("Preparing order dataframe")


    # Handling schema-related issues: fixing timestamp columns

    df_order = df_order.withColumn('order_created_at',F.unix_timestamp(F.lit(df_order.order_created_at),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast("timestamp"))

    parse_and_fix_json = F.udf(validate_json, T.StringType())

    df_order = df_order.withColumn("items_json_text", parse_and_fix_json(F.col("items")))

    # Infering the schema for *items*:
    items_json_schema = sqlc.read.json(df_order.rdd.map(lambda row: row.items_json_text)).schema

    df_order = df_order.withColumn('items', F.from_json(F.col("items_json_text"), items_json_schema)).drop("items_json_text")

    df_order = df_order.withColumn("items", F.col("items").data)


    # Further information can be anonimized here
    sensitive_columns = ["cpf", "customer_name", "delivery_address_latitude", "delivery_address_longitude", "delivery_address_zip_code"]
    df_order = anonimize_columns(df_order, target_columns=sensitive_columns, numBits=256)


    # #### Handling duplicates

    df_order_agg_by_order_id = df_order.groupBy("order_id").agg(F.collect_list("order_created_at").alias("order_created_at_array"), F.collect_list("cpf").alias("cpf_array"))

    df_order_agg_by_order_id = df_order_agg_by_order_id.join(df_order.drop(*["cpf","order_created_at"]).dropDuplicates(), "order_id")


    # Removing duplicated columns

    df_order_agg_by_order_id = df_order_agg_by_order_id.drop(*["customer_name"])


    logging.info("Preparing consumer dataframe")

    # #### Anonymizing sensitive data

    # Further information can be anonimized here
    sensitive_columns = ["customer_name", "customer_phone_number"]
    df_consumer = anonimize_columns(df_consumer, target_columns=sensitive_columns, numBits=256)


    logging.info("Building trusted layer")
  
    # First, lets **avoid column key crashes**:

    df_restaurant = add_prefix_to_df_columns(df_restaurant,"merchant", escape=[])

    df_consumer   = add_prefix_to_df_columns(df_consumer,"customer", escape=[])

    df_status_agg_by_order = add_prefix_to_df_columns(df_status_agg_by_order,"status", escape=['order_id'])  

    logging.info("Building order dataset")

    # #### Joining order with consumer, restaurant, and status
    # Requirement: *one line per order with all data from order, consumer, restaurant*

    df_trusted_order = df_order_agg_by_order_id.join(df_consumer, 'customer_id', "left_outer")

    df_trusted_order = df_trusted_order.join(df_restaurant, 'merchant_id', "left_outer")

    df_trusted_order = df_trusted_order.join(df_status_agg_by_order, 'order_id', "left_outer")



    # #### Adding LOCAL datetime (will be used for partitioning, as required)
    # Requirement: *To help analysis, it would be a nice to have: data partitioned on the restaurant LOCAL date.*

    df_trusted_order = df_trusted_order.withColumn("localtime_order_created_at",pyspark.sql.functions.to_utc_timestamp(F.col("status_last_created_at"), F.col("merchant_timezone")))

    df_trusted_order = df_trusted_order.withColumn("localtime_order_created_at_date", F.to_date(F.col("localtime_order_created_at") ))


    logging.info("Metrics and validation")   
    # As a simple validation, I will check if all ids (customers, merchants, orders, order_id from df_status are in the same final dataframe). I validate this aspect by a simple count.

    

    # df_trusted_order = df_order_agg_by_order_id.join(df_consumer, 'customer_id')
    key_dfs=[df_trusted_order.select("customer_id").where(F.col("customer_id").isNotNull()), 
             df_consumer.select("customer_id")
    ]
    compare_dfs(key_dfs)


    #df_trusted_order = df_trusted_order.join(df_restaurant, 'merchant_id')
    key_dfs=[df_trusted_order.select("merchant_id").where(F.col("merchant_id").isNotNull()), 
             df_restaurant.select("merchant_id")
    ]
    compare_dfs(key_dfs)


    # df_trusted_order = df_trusted_order.join(df_order, 'order_id')
    key_dfs=[df_trusted_order.select("order_id").where(F.col("order_id").isNotNull()), 
             df_order.select("order_id")
    ]
    compare_dfs(key_dfs)


    
    logging.info("Creating datamart")   
    
    df_trusted_order.repartition("localtime_order_created_at_date").write.partitionBy("localtime_order_created_at_date").mode("overwrite").format("parquet").save("s3a://trusted-data/order-dataset.parquet")

    
    logging.info("Done.")   
    
    
    
    
    

def build_order_items_dataset(sc, log_level=logging.INFO):

    logging.basicConfig(level = log_level)
    
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://{}:9000".format(socket.gethostbyname_ex('minio-s3')[2][0]))
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    sqlc = pyspark.sql.SQLContext(sc)

    logging.info("Reading main dataframe") 

    df_orders_trusted_layer = sqlc.read.parquet("s3a://trusted-data/order-dataset.parquet")

    logging.info("Exploding items externalIds and linking to order_ids")
    
    df_trusted_order_items = df_orders_trusted_layer.select('order_id','items').withColumn('items', F.explode('items'))

    df_trusted_order_items = df_trusted_order_items.select("order_id", "items.name", "items.externalId")

    df_trusted_order_items = df_trusted_order_items.groupBy("externalId").agg(F.collect_list("order_id").alias("order_ids"), F.collect_set("name").alias("item_names"))

    logging.info("Creating datamart")   
                 
    df_trusted_order_items.repartition(1).write.format("parquet").mode("append").save("s3a://trusted-data/order_items-dataset.parquet")
                 
    logging.info("Done.")   

                 
                             
                 
                 
                 

    

def build_order_statuses_dataset(sc, log_level=logging.INFO):

    logging.basicConfig(level = log_level)
        
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://{}:9000".format(socket.gethostbyname_ex('minio-s3')[2][0]))
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    sqlc = pyspark.sql.SQLContext(sc)

    logging.info("Reading main dataframes") 
                 
    df_orders_trusted_layer = sqlc.read.parquet("s3a://trusted-data/order-dataset.parquet")

    df_status_raw = sqlc.read.parquet('s3a://raw-data/status.json.parquet')

    logging.info("Pivoting on values")
                 
    df_status_raw = df_status_raw.groupBy('order_id').pivot("value").agg(F.collect_set("created_at"))

    logging.info("Merging with missing ids")

    df_status_raw = df_status_raw.join(df_orders_trusted_layer.select("order_id"), 'order_id', "right_outer")

    logging.info("Creating datamart")   
 
    df_status_raw.repartition(1).write.format("parquet").mode("append").save("s3a://trusted-data/order_statuses-dataset.parquet")
                 
    logging.info("Done.")   
                 