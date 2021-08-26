import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import row_number, col, upper, trim, round, from_utc_timestamp
from awsglue.utils import getResolvedOptions
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, BooleanType, IntegerType

def load_cases(case_df):
    time_cols = ['closeddate', 'createddate', 'lastmodifieddate', 'lastreferenceddate', 'lastvieweddate',
                 'date_time_answered__c', 'enddate__c', 'exact_creation_date__c', 'planned_end_date__c', 'startdate__c',
                 'start_date__c', 'etlinserttime', 'date_time_reopened__c']

    for col_name in time_cols:
        case_df = case_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))

    boolean_cols = ['isclosed', 'isdeleted', 'isescalated', 'call_me_back__c', 'x2nd_line_help__c']

    for col_name in boolean_cols:
        case_df = case_df.withColumn(col_name, trim(col(col_name)).cast(BooleanType()))

    case_df = case_df.withColumn('exact_id__c', upper(trim(col('exact_id__c'))))
    case_df = case_df.withColumn('number_of_times_reopened__c', col('number_of_times_reopened__c').cast(IntegerType()))

    window_spec_case = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                             col('etlinserttime').desc())
    case_df = case_df.withColumn("row_num", row_number().over(window_spec_case))
    case_df = case_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth', 'ingestionday')

    case_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_case'])

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['raw_db', 'raw_table_case', 's3_destination_case'])

    glueContext = GlueContext(SparkContext.getOrCreate())

    raw_case_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                            table_name=args['raw_table_case'],
                                                            additional_options={"mergeSchema": "true"}).toDF()

    load_cases(raw_case_df)