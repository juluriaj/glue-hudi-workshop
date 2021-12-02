from pyspark.sql.types import StringType
import sys
import os
import json

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp, dense_rank, desc
from pyspark.sql.window import Window

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import boto3
from botocore.exceptions import ClientError

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAWZONE_BUCKET', 'CURATED_BUCKET'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

logger.info('Initialization.')
glueClient = boto3.client('glue')

logger.info('Fetching configuration.')
region = os.environ['AWS_DEFAULT_REGION']

rawBucketName = args['RAWZONE_BUCKET']
curatedBucketName = args['CURATED_BUCKET']

if rawBucketName == None or curatedBucketName == None:
    raise Exception("Please input the bucket names in job parameters. Refer: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-get-resolved-options.html")

rawS3TablePath = 's3://' + rawBucketName + '/tickets/dms_sample/ticket_purchase_hist/'
curatedS3TablePathPrefix = 's3://' + curatedBucketName + '/hudi/'

sourceDBName = 'dms_sample'
sourceTableName = 'ticket_purchase_hist'

targetDBName = 'hudi_sample'
targetTableName = 'ticket_purchase_hist'

hudiStorageType = 'CoW'

dropColumnList = ['db','table_name','Op']
      
logger.info('Processing starts.')

keys = {
    "dms_sample.ticket_purchase_hist": {"primaryKey": "sporting_event_ticket_id"}
    }

spark.sql('CREATE DATABASE IF NOT EXISTS ' + targetDBName)

isTableExists = False
isPrimaryKey = False
isPartitionKey = False
primaryKey = ''
partitionKey = ''
try:
    glueClient.get_table(DatabaseName=targetDBName,Name=targetTableName)
    isTableExists = True
    logger.info(targetDBName + '.' + targetTableName + ' exists.')
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        isTableExists = False
        logger.info(targetDBName + '.' + targetTableName + ' does not exist. Table will be created.')

# lookup primary key and partition keys from keys json declared above
try:
    keyName = sourceDBName +"." + sourceTableName
    logger.info(keyName)
    table_config = ''
    
    for key in keys:
        if key == keyName:
            table_config = keys[key]
    
    try:
        primaryKey = table_config['primaryKey']
        isPrimaryKey = True
        logger.info('Primary key:' + primaryKey)
    except KeyError as e:
        isPrimaryKey = False
        logger.info('Primary key not found. An append only glueparquet table will be created.')
    try:
        partitionKey = table_config['partitionKey']
        isPartitionKey = True
        logger.info('Partition key:' + partitionKey)
    except KeyError as e:
        isPartitionKey = False
        logger.info('Partition key not found. Partitions will not be created.')
except ClientError as e:    
    if e.response['Error']['Code'] == 'ParameterNotFound':
        isPrimaryKey = False
        isPartitionKey = False
        logger.info('Config for ' + sourceDBName + '.' + sourceTableName + ' not found in parameter store. Non partitioned append only table will be created.')

# Reads the raw zone table and writes to HUDI table
try:
    inputDyf = glueContext.create_dynamic_frame_from_options(connection_type = 's3', connection_options = {'paths': [rawS3TablePath], 'groupFiles': 'none', 'recurse':True}, format = 'csv', format_options={'withHeader':True}, transformation_ctx = targetTableName)
    
    # Ensure timestamp is in HUDI timestamp format
    inputDf = inputDyf.toDF().withColumn('transaction_date_time', to_timestamp(col('transaction_date_time'))).withColumn(primaryKey, col(primaryKey).cast(StringType()))

    logger.info('Total record count in raw table = ' + str(inputDyf.count()))

    targetPath = curatedS3TablePathPrefix + '/' + targetDBName + '/' + targetTableName

    morConfig = {'hoodie.datasource.write.storage.type': 'MERGE_ON_READ', 'hoodie.compact.inline': 'false', 'hoodie.compact.inline.max.delta.commits': 20, 'hoodie.parquet.small.file.limit': 0}

    commonConfig = {'className' : 'org.apache.hudi', 'hoodie.datasource.hive_sync.use_jdbc':'false', 'hoodie.datasource.write.precombine.field': 'transaction_date_time', 'hoodie.datasource.write.recordkey.field': primaryKey, 'hoodie.table.name': targetTableName, 'hoodie.consistency.check.enabled': 'true', 'hoodie.datasource.hive_sync.database': targetDBName, 'hoodie.datasource.hive_sync.table': targetTableName, 'hoodie.datasource.hive_sync.enable': 'true'}

    partitionDataConfig = {'hoodie.datasource.write.partitionpath.field': partitionKey, 'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor', 'hoodie.datasource.hive_sync.partition_fields': partitionKey}
                    
    unpartitionDataConfig = {'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor', 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}
    
    incrementalConfig = {'hoodie.upsert.shuffle.parallelism': 20, 'hoodie.datasource.write.operation': 'upsert', 'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10}
    
    initLoadConfig = {'hoodie.bulkinsert.shuffle.parallelism': 3, 'hoodie.datasource.write.operation': 'bulk_insert'}
    
    deleteDataConfig = {'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.EmptyHoodieRecordPayload'}

    if(hudiStorageType == 'MoR'):
        commonConfig = {**commonConfig, **morConfig}
        logger.info('MoR config appended to commonConfig.')
    
    combinedConf = {}

    # HUDI require us to provide a primaryKey. If no primaryKey defined, we will fallback to 'glueparquet' format
    if(isPrimaryKey):
        logger.info('Going the Hudi way.')
        if(isTableExists):
            logger.info('Incremental load.')

            # ensure only latest updates are kept for each record using descending timestamp order
            w = Window.partitionBy(primaryKey).orderBy(desc('transaction_date_time'))
            inputDf = inputDf.withColumn('Rank',dense_rank().over(w))
            inputDf = inputDf.filter(inputDf.Rank == 1).drop(inputDf.Rank)

            outputDf = inputDf.filter("Op != 'D'").drop(*dropColumnList)

            if outputDf.count() > 0:
                logger.info('Upserting data.')
                if (isPartitionKey):
                    logger.info('Writing to partitioned Hudi table.')
                    outputDf = outputDf.withColumn(partitionKey,concat(lit(partitionKey+'='),col(partitionKey)))
                    combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig}
                    outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('Append').save(targetPath)
                else:
                    logger.info('Writing to unpartitioned Hudi table.')
                    combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig}
                    outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('Append').save(targetPath)
            
            outputDf_deleted = inputDf.filter("Op = 'D'").drop(*dropColumnList)

            if outputDf_deleted.count() > 0:
                logger.info('Some data got deleted.')
                if (isPartitionKey):
                    logger.info('Deleting from partitioned Hudi table.')
                    outputDf_deleted = outputDf_deleted.withColumn(partitionKey,concat(lit(partitionKey+'='),col(partitionKey)))
                    combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig, **deleteDataConfig}
                    outputDf_deleted.write.format('org.apache.hudi').options(**combinedConf).mode('Append').save(targetPath)
                else:
                    logger.info('Deleting from unpartitioned Hudi table.')
                    combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig, **deleteDataConfig}
                    outputDf_deleted.write.format('org.apache.hudi').options(**combinedConf).mode('Append').save(targetPath)
        else:
            outputDf = inputDf.drop(*dropColumnList)
            if outputDf.count() > 0:
                logger.info('Inital load.')
                if (isPartitionKey):
                    logger.info('Writing to partitioned Hudi table.')
                    outputDf = outputDf.withColumn(partitionKey,concat(lit(partitionKey+'='),col(partitionKey)))
                    combinedConf = {**commonConfig, **partitionDataConfig, **initLoadConfig}
                    outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('Overwrite').save(targetPath)
                else:
                    logger.info('Writing to unpartitioned Hudi table.')
                    combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}
                    outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('Overwrite').save(targetPath)
    else:
        if (isPartitionKey):
            logger.info('Writing to partitioned glueparquet table.')
            sink = glueContext.getSink(connection_type = 's3', path= targetPath, enableUpdateCatalog = True, updateBehavior = 'UPDATE_IN_DATABASE', partitionKeys=[partitionKey])
        else:
            logger.info('Writing to unpartitioned glueparquet table.')
            sink = glueContext.getSink(connection_type = 's3', path= targetPath, enableUpdateCatalog = True, updateBehavior = 'UPDATE_IN_DATABASE')
        sink.setFormat('glueparquet')
        sink.setCatalogInfo(catalogDatabase = targetDBName, catalogTableName = targetTableName)
        outputDyf = DynamicFrame.fromDF(inputDf.drop(*dropColumnList), glueContext, 'outputDyf')
        sink.writeFrame(outputDyf)
except BaseException as e:
    logger.info('An error occurred while processing table ' + targetDBName + '.' + targetTableName + '. Please check the error logs...')
    print(e)

job.commit()
