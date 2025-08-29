import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
import logging

logger=logging.getLogger("my_logger")
logger.setLevel(logging.INFO)


# Create a handler for CloudWatch
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info('My log message')
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1756452497645 = glueContext.create_dynamic_frame.from_catalog(database="mydatabase", table_name="customer", transformation_ctx="AmazonS3_node1756452497645")

logger.info('print schema of AmazonS3_node1756452497645')
AmazonS3_node1756452497645.printSchema()


count = AmazonS3_node1756452497645.count()
print("Number of rows in AmazonS3_node1756452497645 dynamic frame: ", count)
logger.info('count for frame is {}'.format(count))

# Script generated for node Change Schema
ChangeSchema_node1756452536622 = ApplyMapping.apply(frame=AmazonS3_node1756452497645, mappings=[("index", "long", "index", "long"), ("customer id", "string", "new_customer id", "string"), ("first name", "string", "new_first name", "string"), ("last name", "string", "last name", "string"), ("company", "string", "company", "string"), ("city", "string", "city", "string"), ("country", "string", "country", "string"), ("phone 1", "string", "phone 1", "string"), ("phone 2", "string", "new_phone 2", "string"), ("email", "string", "email", "string"), ("subscription date", "string", "subscription date", "string"), ("website", "string", "website", "string")], transformation_ctx="ChangeSchema_node1756452536622")

#convert those string values to long values using the resolveChoice transform method with a cast:long option:
#This replaces the string values with null values

ResolveChoice_node = ChangeSchema_node1756452536622.resolveChoice(specs = [('new_phone 2','cast:long')],
transformation_ctx="ResolveChoice_node"
)


logger.info('print schema of ResolveChoice_node')
ResolveChoice_node.printSchema()


#convert dynamic dataframe into spark dataframe
logger.info('convert dynamic dataframe ResolveChoice_node into spark dataframe')

spark_data_frame=ResolveChoice_node.toDF()


#removing the space from column names spark 
logger.info('renaming the column names')
spark_data_frame_filter = spark_data_frame.withColumnRenamed("new_phone 2","new_phone_2").withColumnRenamed("new_first name","new_first_name").withColumnRenamed("new_customer id","new_customer_id")

#apply spark where clause
logger.info('filter rows with  where new_seller_id is not null')
spark_data_frame_filter = spark_data_frame_filter.where("new_phone_2 is NOT NULL")


# Add the new column to the data frame
logger.info('create new column status with Active value')
spark_data_frame_filter = spark_data_frame_filter.withColumn("new_status", lit("Active"))

spark_data_frame_filter.show()


logger.info('convert spark dataframe into table view product_view. so that we can run sql ')
spark_data_frame_filter.createOrReplaceTempView("customer_view")


logger.info('create dataframe by spark sql ')

customer_sql_df = spark.sql("SELECT new_first_name,count(new_customer_id) as cnt,sum(index) as qty FROM customer_view group by new_first_name")


logger.info('display records after aggregate result')
customer_sql_df.show()

# Convert the data frame back to a dynamic frame
logger.info('convert spark dataframe to dynamic frame ')
dynamic_frame = DynamicFrame.fromDF(customer_sql_df, glueContext, "dynamic_frame")


S3bucket_node3 = glueContext.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-myglueetl/output/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3bucket_node3")

job.commit()