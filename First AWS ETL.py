import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_format_timestamp
from awsglue.gluetypes import *
import gs_to_timestamp
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node = glueContext.create_dynamic_frame.from_catalog(database="your_database_name_here", table_name="your_table_name_here", transformation_ctx="AWSGlueDataCatalog_node")

# Script generated for node SQL Query
SqlQuery = '''
select Title, Score, Upvote_Ratio, Number_of_Comments, Url, Author, Flair, FROM_UNIXTIME(Created_UTC) as Year from myDataSource order by Year
'''
SQLQuery_node = sparkSqlQuery(glueContext, query=SqlQuery, mapping={"myDataSource": AWSGlueDataCatalog_node}, transformation_ctx="SQLQuery_node")

# Script generated for node To Timestamp
ToTimestamp_node = SQLQuery_node.gs_to_timestamp(colName="Year", colType="autodetect")

# Script generated for node Format Timestamp
FormatTimestamp_node = ToTimestamp_node.gs_format_timestamp(colName="Year", dateFormat="yyyy-MM-dd")

# Script generated for node Drop Null Fields
DropNullFields_node = drop_nulls(glueContext, frame=FormatTimestamp_node, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node")

# Script generated for node Amazon S3
AmazonS3_node = glueContext.write_dynamic_frame.from_options(frame=DropNullFields_node, connection_type="s3", format="csv", connection_options={"path": "s3://your_bucket_name_here/your_path_here/", "compression": "gzip", "partitionKeys": []}, transformation_ctx="AmazonS3_node")

job.commit()