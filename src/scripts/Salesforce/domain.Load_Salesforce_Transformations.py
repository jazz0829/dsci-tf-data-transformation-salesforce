import sys
from utils import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import row_number, col, upper, trim, round, from_utc_timestamp
from awsglue.utils import getResolvedOptions
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, BooleanType, FloatType, IntegerType, DateType

def load_accounts():
    account_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                               table_name=args['raw_table_account'],
                                                               additional_options={"mergeSchema": "true"}).toDF()

    time_cols = ['lastmodifieddate', 'etlinserttime', 'enddate__c']
    for col_name in time_cols:
        account_df = account_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))

    boolean_cols = ['controlledrelease__c', 'ispartner', 'active__c']
    for col_name in boolean_cols:
        account_df = account_df.withColumn(col_name, trim(col(col_name)).cast(BooleanType()))

    account_df = account_df.withColumn('exact_id__c', upper(trim(col('exact_id__c'))))
    account_df = account_df.withColumn('contract_value__c', round(col('contract_value__c').cast(FloatType()), 2))

    window_spec_account = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                                col('etlinserttime').desc())
    account_df = account_df.withColumn("row_num", row_number().over(window_spec_account))
    account_df = account_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth',
                                                            'ingestionday')
    account_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_account'])

def load_case_comments():
    case_comment_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                                    table_name=args['raw_table_case_comment'],
                                                                    additional_options={"mergeSchema": "true"}).toDF()
    time_cols = ['lastmodifieddate', 'createddate', 'etlinserttime']
    for col_name in time_cols:
        case_comment_df = case_comment_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))

    window_spec_case_comment = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                                     col('etlinserttime').desc())
    case_comment_df = case_comment_df.withColumn("row_num", row_number().over(window_spec_case_comment))
    case_comment_df = case_comment_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth',
                                                                      'ingestionday')
    case_comment_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_case_comment'])

def load_users():
    user_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'], table_name=args['raw_table_user'],
                                                            additional_options={"mergeSchema": "true"}).toDF()

    user_df = user_df.withColumn('userid__c', upper(trim(col('userid__c'))))
    user_df = user_df.withColumn('exactidaccount__c', upper(trim(col('exactidaccount__c'))))

    time_cols = ['lastmodifieddate', 'etlinserttime']
    for col_name in time_cols:
        user_df = user_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))
    user_df = user_df.withColumn('lastmodifieddate', col('lastmodifieddate').cast(TimestampType()))
    user_df = user_df.withColumn('etlinserttime', col('etlinserttime').cast(TimestampType()))

    window_spec_user = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                             col('etlinserttime').desc())
    user_df = user_df.withColumn("row_num", row_number().over(window_spec_user))
    user_df = user_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth', 'ingestionday')
    user_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_user'])

def load_record_types():
    record_type_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                                   table_name=args['raw_table_record_type'],
                                                                   additional_options={"mergeSchema": "true"}).toDF()
    time_cols = ['systemmodstamp', 'lastmodifieddate', 'etlinserttime']
    for col_name in time_cols:
        record_type_df = record_type_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))

    record_type_df = record_type_df.withColumn('isactive', trim(col('isactive')).cast(BooleanType()))

    window_spec_record_type = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                                    col('etlinserttime').desc())
    record_type_df = record_type_df.withColumn("row_num", row_number().over(window_spec_record_type))
    record_type_df = record_type_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth',
                                                                    'ingestionday')
    record_type_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_record_type'])

def load_contacts():
    contact_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                               table_name=args['raw_table_contact'],
                                                               additional_options={"mergeSchema": "true"}).toDF()

    time_cols = ['createddate', 'lastactivitydate', 'lastcurequestdate', 'lastcuupdatedate', 'lastmodifieddate',
                 'lastvieweddate', 'startdate__c', 'etlinserttime']
    for col_name in time_cols:
        contact_df = contact_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))

    contact_df = contact_df.withColumn('isdeleted', trim(col('isdeleted')).cast(BooleanType()))
    contact_df = contact_df.withColumn('isemailbounced', trim(col('isemailbounced')).cast(BooleanType()))
    contact_df = contact_df.withColumn('enddate__c', col('enddate__c').cast(TimestampType()))
    contact_df = contact_df.withColumn('userid__c', upper(trim(col('userid__c'))))
    contact_df = contact_df.withColumn('exact_id__c', upper(trim(col('exact_id__c'))))

    window_spec_contact = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                                col('etlinserttime').desc())
    contact_df = contact_df.withColumn("row_num", row_number().over(window_spec_contact))
    contact_df = contact_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth',
                                                            'ingestionday')
    contact_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_contact'])

def load_topics():
    topic_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                             table_name=args['raw_table_topic'],
                                                             additional_options={"mergeSchema": "true"}).toDF()
    time_cols = ['systemmodstamp', 'lastmodifieddate', 'createddate', 'etlinserttime']
    for col_name in time_cols:
        topic_df = topic_df.withColumn(col_name, col(col_name).cast(TimestampType()))
    topic_df = topic_df.withColumn('talkingabout', col('talkingabout').cast(IntegerType()))

    window_spec_topic = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                              col('etlinserttime').desc())
    topic_df = topic_df.withColumn("row_num", row_number().over(window_spec_topic))
    topic_df = topic_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth', 'ingestionday')
    topic_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_topic'])

def load_topic_assignments():
    topic_assignment_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                                        table_name=args['raw_table_topic_assignment'],
                                                                        additional_options={
                                                                            "mergeSchema": "true"}).toDF()

    time_cols = ['systemmodstamp', 'lastmodifieddate', 'createddate', 'etlinserttime']
    for col_name in time_cols:
        topic_assignment_df = topic_assignment_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))

    topic_assignment_df = topic_assignment_df.withColumn('isdeleted', col('isdeleted').cast(BooleanType()))

    window_spec_topic_assignment = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                                         col('etlinserttime').desc())
    topic_assignment_df = topic_assignment_df.withColumn("row_num", row_number().over(window_spec_topic_assignment))
    topic_assignment_df = topic_assignment_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear',
                                                                              'ingestionmonth',
                                                                              'ingestionday')
    topic_assignment_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_topic_assignment'])

def load_knowledge_article_version():
    kav_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                           table_name=args['raw_table_knowledgearticleversion'],
                                                           additional_options={"mergeSchema": "true"}).toDF()
    time_cols = ['articlecreateddate', 'lastmodifieddate', 'createddate', 'etlinserttime', 'firstpublisheddate',
                 'lastpublisheddate']
    for col_name in time_cols:
        kav_df = kav_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))

    window_spec_kav = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                            col('etlinserttime').desc())
    kav_df = kav_df.withColumn("row_num", row_number().over(window_spec_kav))
    kav_df = kav_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth',
                                                    'ingestionday')
    kav_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_knowledgearticleversion'])

def load_surveys():
    survey_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                              table_name=args['raw_table_survey'],
                                                              additional_options={"mergeSchema": "true"}).toDF()
    time_cols = ['lastmodifieddate', 'lastactivitydate', 'createddate', 'etlinserttime']
    for col_name in time_cols:
        survey_df = survey_df.withColumn(col_name, trim(col(col_name)).cast(TimestampType()))

    integer_cols = ['answer_1__c', 'answer_2__c', 'answer_3__c', 'answer_4__c', 'answer_5__c', 'answer_6__c',
                    'answer_7__c', 'new_answer_1__c', 'new_answer_4__c']
    for col_name in integer_cols:
        survey_df = survey_df.withColumn(col_name, trim(col(col_name)).cast(IntegerType()))

    float_cols = ['average_a1_a4__c', 'average_a4_7__c', 'converted_answer_1__c', 'converted_answer_2__c',
                  'converted_answer_3__c', 'converted_answer_4__c','new_converted_answer_4__c',
                  'converted_answer_5__c', 'converted_answer_6__c', 'converted_answer_7__c',"new_converted_answer_1__c"]
    for col_name in float_cols:
        survey_df = survey_df.withColumn(col_name, trim(col(col_name)).cast(FloatType()))

    boolean_cols = ['detractor__c','isdeleted','promoter__c']
    for col_name in boolean_cols:
        survey_df = survey_df.withColumn(col_name, trim(col(col_name)).cast(BooleanType()))

    window_spec_survey = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(),
                                                               col('etlinserttime').desc())
    survey_df = survey_df.withColumn("row_num", row_number().over(window_spec_survey))
    survey_df = survey_df.where(col('row_num') == 1).drop('row_num', 'ingestionyear', 'ingestionmonth',
                                                          'ingestionday')
    survey_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_survey'])

def load_support_cases():
    spark = SparkSession.builder.getOrCreate()
    case_df = spark.read.parquet(args['s3_destination_case'])
    user_df = spark.read.parquet(args['s3_destination_user'])
    account_df = spark.read.parquet(args['s3_destination_account'])
    record_type_df = spark.read.parquet(args['s3_destination_record_type'])
    contact_df = spark.read.parquet(args['s3_destination_contact'])

    case_reduced_df = case_df.select("id", "casenumber", "account_hosting_environment__c", "origin", "createddate",
                                     "closeddate", "type", "main_reason__c", "sub_reason__c", "subject", "description",
                                     "solution__c", "x2nd_line_help__c", "date_time_reopened__c", "date_time_answered__c",
                                     "owner_role__c", "number_of_times_reopened__c", "status", "reason_complaint__c",
                                     "sub_reason_complaint__c", "ownerid", "accountid", "contactid", "recordtypeid").alias(
        "case")
    user_df = user_df.withColumnRenamed("name", "case_owner").withColumnRenamed("alias", "case_owner_alias")
    account_df = account_df.withColumnRenamed("subscription__c", "subscription").withColumnRenamed("accountnumber",
                                                                                                   "accountcode")
    record_type_df = record_type_df.withColumnRenamed("name", "case_record_type")
    user_df = user_df.alias('user')
    account_df = account_df.alias('acc')
    record_type_df = record_type_df.alias('rec')
    contact_df = contact_df.alias('contact')
    supportcase_df = case_reduced_df.join(user_df, (case_reduced_df.ownerid == user_df.id), how='left').select("case.*",
                                                                                                               "user.case_owner",
                                                                                                               "user.case_owner_alias")
    supportcase_df = supportcase_df.alias('supportcase')
    supportcase_df = supportcase_df.join(account_df, (supportcase_df.accountid == account_df.id), how='left').select(
        "supportcase.*", "acc.subscription", "acc.accountcode", "acc.exact_id__c")
    supportcase_df = supportcase_df.join(contact_df, (supportcase_df.contactid == contact_df.id), how='left').select(
        "supportcase.*", "contact.userid__c")
    supportcase_df = supportcase_df.join(record_type_df, (supportcase_df.recordtypeid == record_type_df.id),
                                         how='left').select("supportcase.*", "rec.case_record_type")
    supportcase_df = supportcase_df.withColumnRenamed("id", "caseid").withColumnRenamed("exact_id__c", "accountid") \
        .withColumnRenamed('account_hosting_environment__c', 'environment').withColumnRenamed('userid__c', 'userid') \
        .withColumnRenamed('main_reason__c', 'main_reason').withColumnRenamed('sub_reason__c', 'sub_reason') \
        .withColumnRenamed('x2nd_line_help__c', 'second_line_support').withColumnRenamed('date_time_reopened__c',
                                                                                         'reopen_date') \
        .withColumnRenamed('date_time_answered__c', 'answered_date').withColumnRenamed('owner_role__c', 'case_owner_role') \
        .withColumnRenamed('number_of_times_reopened__c', 'number_of_times_reopened').withColumnRenamed(
        'reason_complaint__c', 'main_reason_complaint') \
        .withColumnRenamed('sub_reason_complaint__c', 'sub_reason_complaint').withColumnRenamed('solution__c', 'solution')
    supportcase_df = supportcase_df.withColumn('opened_date',
                                               from_utc_timestamp(supportcase_df.createddate, 'Europe/Amsterdam'))
    supportcase_df = supportcase_df.withColumn('opened_datetime',
                                               from_utc_timestamp(supportcase_df.createddate, 'Europe/Amsterdam').cast(
                                                   DateType()))
    supportcase_df = supportcase_df.withColumn('closed_date',
                                               from_utc_timestamp(supportcase_df.closeddate, 'Europe/Amsterdam'))
    supportcase_df = supportcase_df.withColumn('closed_datetime',
                                               from_utc_timestamp(supportcase_df.closeddate, 'Europe/Amsterdam').cast(
                                                   DateType()))
    supportcase_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_supportcases_salesforce'])

def load_salesforce_webactivity():
    salesforce_webactivity_raw_df = glueContext.create_dynamic_frame.from_catalog(database=args['raw_db'],
                                                              table_name=args['raw_table_salesforce_webactivity']).toDF()

    salesforce_webactivity_raw_df = cleanDataFrame(salesforce_webactivity_raw_df, ['id'])

    salesforce_webactivity_raw_df = convertDataTypes(
            data_frame = salesforce_webactivity_raw_df,
            timestamp_cols = ['createddate', 'lastmodifieddate', 'lastreferenceddate', 'lastvieweddate', 'etlinserttime'],
            boolean_cols = ['isdeleted', 'isprobablyaccountant__c']
        )


    window_spec = Window.partitionBy(col('id')).orderBy(col('lastmodifieddate').desc(), col('etlinserttime').desc())
    salesforce_webactivity_df = salesforce_webactivity_raw_df.withColumn("row_num", row_number().over(window_spec))
    salesforce_webactivity_df = salesforce_webactivity_df.where(col('row_num') == 1)

    salesforce_webactivity_df = salesforce_webactivity_df.drop('row_num', 'ingestionday', 'ingestionmonth', 'ingestionyear')

    salesforce_webactivity_df.repartition(1).write.mode("overwrite").parquet(args['s3_destination_salesforce_webactivity'])

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['raw_db','s3_destination_case','s3_destination_supportcases_salesforce',
                                         'raw_table_account', 's3_destination_account', 'raw_table_case_comment',
                                         's3_destination_case_comment', 'raw_table_user', 's3_destination_user',
                                         'raw_table_record_type', 's3_destination_record_type', 'raw_table_contact',
                                         's3_destination_contact', 'raw_table_topic', 'raw_table_salesforce_webactivity', 's3_destination_topic',
                                         'raw_table_topic_assignment', 's3_destination_topic_assignment', 's3_destination_salesforce_webactivity',
                                         'raw_table_knowledgearticleversion', 's3_destination_knowledgearticleversion',
                                         'raw_table_survey', 's3_destination_survey'])

    glueContext = GlueContext(SparkContext.getOrCreate())
    load_accounts()
    load_contacts()
    load_knowledge_article_version()
    load_record_types()
    load_users()
    load_surveys()
    load_accounts()
    load_case_comments()
    load_topics()
    load_topic_assignments()
    load_support_cases()
    load_salesforce_webactivity()