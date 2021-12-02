import os
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CURATED_BUCKET'])

spark = SparkSession.builder.config(
    'spark.serializer',
    'org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

logger.info('Initialization.')
glueClient = boto3.client('glue')

logger.info('Fetching configuration.')
region = os.environ['AWS_DEFAULT_REGION']

curatedBucketName = args['CURATED_BUCKET']

if curatedBucketName == None:
    raise Exception(
        "Please input the bucket names in job parameters. Refer: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-get-resolved-options.html"
    )

keys = {
    "dms_sample.ticket_purchase_hist": {"primaryKey": "sporting_event_ticket_id"}
    }

curatedS3TablePathPrefix = 's3://' + curatedBucketName + '/hudi/'

hudiDBName = 'hudi_sample'
hudiTableName = 'ticket_purchase_hist'
primaryKey = 'sporting_event_ticket_id'

unpartitionDataConfig = {
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
}

incrementalConfig = {
    'hoodie.upsert.shuffle.parallelism': 68,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
    'hoodie.cleaner.commits.retained': 10
}

all_commits = list(
    map(
        lambda row: row[0],
        spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from " + hudiDBName + ".ticket_purchase_hist order by commitTime"
        ).limit(50).collect()))

logger.info('Total number of commits are: ' + str(len(all_commits)))
beginTime = all_commits[len(all_commits) - 1]  # commit time we are interested in

# incrementally query data
incremental_read_options = {
    'hoodie.datasource.query.type': 'incremental',
    'hoodie.datasource.read.begin.instanttime': beginTime,
}

incrementalDF = spark.read.format("hudi").options(**incremental_read_options). \
  load(curatedS3TablePathPrefix + hudiDBName + '/' + hudiTableName)

commonConfig = {
    'className': 'org.apache.hudi',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.write.precombine.field': 'transaction_date_time',
    'hoodie.datasource.write.recordkey.field': primaryKey,
    'hoodie.table.name': hudiTableName + '_incremental',
    'hoodie.consistency.check.enabled': 'true',
    'hoodie.datasource.hive_sync.database': hudiDBName,
    'hoodie.datasource.hive_sync.table': hudiTableName + '_incremental',
    'hoodie.datasource.hive_sync.enable': 'true'
}

combinedConf = { **commonConfig, **unpartitionDataConfig, **incrementalConfig }

incrementalDF.write.format('org.apache.hudi').options(**combinedConf).mode('Overwrite').save(curatedS3TablePathPrefix + '/' + hudiDBName + '/' + hudiTableName + '_incremental')
job.commit()