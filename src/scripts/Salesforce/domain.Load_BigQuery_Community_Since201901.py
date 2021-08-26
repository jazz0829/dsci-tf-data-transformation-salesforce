import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import row_number, col, trim, from_utc_timestamp
from awsglue.utils import getResolvedOptions
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType, DateType

def load_bigquery_community_since201901(bigquery_df, salesforce_user_df, users_df):
    integer_cols = ["visitId", "visitStartTime", "hits_hitNumber", "totals_hits", "totals_pageviews", 
                    "totals_timeOnSite", "hits_eventInfo_eventValue", "hits_time"]

    for col_name in integer_cols:
        bigquery_df = bigquery_df.withColumn(col_name, trim(col(col_name)).cast(IntegerType()))

    window_spec = Window.partitionBy(col("visitId"), col("fullVisitorId"), col("hits_hitNumber")).orderBy(col("hits_hitNumber"))
    community_df = bigquery_df.withColumn("rownr", row_number().over(window_spec))
    community_df = community_df.where(col('rownr') == 1)

    community_df = community_df.select("userIdLong", "visitId", "hits_hitNumber", "totals_hits", "totals_pageviews", "totals_timeOnSite", 
                                      "hits_page_pagePath", "hits_page_hostname", "hits_page_pageTitle", "hits_page_searchKeyword", 
                                      "hits_page_searchCategory", "hits_page_pagePathLevel3", "hits_page_pagePathLevel4", "hits_eventInfo_eventCategory", 
                                      "hits_eventInfo_eventAction", "hits_eventInfo_eventLabel", "hits_eventInfo_eventValue", "hits_time", "visitStartTime")
    
    visitStartTimeUTC = (col("VisitStartTime") + (col("hits_time")/1000)).cast(TimestampType())
    community_df = community_df.withColumn("visitStartTimeUTC", visitStartTimeUTC)

    visitStartTimeCET = from_utc_timestamp(visitStartTimeUTC, 'Europe/Amsterdam')
    community_df = community_df.withColumn("visitStartTimeCET", visitStartTimeCET)
    community_df = community_df.withColumn("visitStartDateCET", visitStartTimeCET.cast(DateType()))
    
    community_df = community_df.alias('BQ').join(salesforce_user_df.alias('SFUL'), col('SFUL.Id') == col('BQ.userIdLong'))
    
    community_df = community_df.join(users_df.alias('US'), col('US.UserID') == col('SFUL.UserID__c'))
    
    community_df = community_df.select("BQ.userIdLong", "BQ.visitStartTimeUTC", "BQ.visitStartTimeCET", "BQ.visitStartDateCET", "BQ.visitId", "BQ.hits_hitNumber", 
                                       "BQ.totals_hits", "BQ.totals_pageviews", "BQ.totals_timeOnSite", "BQ.hits_page_pagePath", "BQ.hits_page_hostname", 
                                       "BQ.hits_page_pageTitle", "BQ.hits_page_searchKeyword", "BQ.hits_page_searchCategory", "BQ.hits_page_pagePathLevel4", 
                                       "BQ.hits_eventInfo_eventCategory", "BQ.hits_eventInfo_eventAction", "BQ.hits_eventInfo_eventLabel", "BQ.hits_eventInfo_eventValue", 
                                       "SFUL.UserID__c", "US.AccountID", "US.Environment", "BQ.hits_time", "BQ.hits_page_pagePathLevel3")
    
    community_df = community_df.withColumnRenamed("UserID__c", "EOLUserID")

    community_df.repartition(10).write.mode("overwrite").parquet(args['s3_destination_bigquery'])

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['raw_db', 'domain_db', 'raw_table_bigquery',
                                         'domain_table_salesforce_user', 'domain_table_users', 's3_destination_bigquery'])

    glueContext = GlueContext(SparkContext.getOrCreate())

    raw_bigquery_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                            table_name=args['raw_table_bigquery']).toDF()

    domain_salesforce_user_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'],
                                                            table_name=args['domain_table_salesforce_user']).toDF()

    domain_users_df = glueContext.create_dynamic_frame.from_catalog(database=args['domain_db'],
                                                            table_name=args['domain_table_users']).toDF()

    load_bigquery_community_since201901(raw_bigquery_df, domain_salesforce_user_df, domain_users_df)