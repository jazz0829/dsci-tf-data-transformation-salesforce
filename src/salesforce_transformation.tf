resource "aws_s3_bucket_object" "Salesforce_Cases" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Salesforce/domain.Load_Salesforce_Cases.py"
  source = "scripts/Salesforce/domain.Load_Salesforce_Cases.py"
  etag   = "${md5(file("scripts/Salesforce/domain.Load_Salesforce_Cases.py"))}"
}

resource "aws_glue_job" "Load_Salesforce_Cases" {
  name               = "domain.Load_Salesforce_Cases"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Salesforce_Cases.id}"
  }

  default_arguments = {
    "--raw_db"                                 = "${var.raw_database}"
    "--raw_table_case"                         = "${var.raw_salesforce_case_table}"
    "--s3_destination_case"                    = "s3://${var.domain_bucket}/Data/Salesforce_Case"
    "--job-bookmark-option"                    = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Salesforce_Transformations" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Salesforce/domain.Load_Salesforce_Transformations.py"
  source = "scripts/Salesforce/domain.Load_Salesforce_Transformations.py"
  etag   = "${md5(file("scripts/Salesforce/domain.Load_Salesforce_Transformations.py"))}"
}

resource "aws_glue_job" "Load_Salesforce_Transformations" {
  name               = "domain.Load_Salesforce_Transformations"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Salesforce_Transformations.id}"
  }

  default_arguments = {
    "--extra-py-files"                         = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                                 = "${var.raw_database}"
    "--raw_table_account"                      = "${var.raw_salesforce_account_table}"
    "--s3_destination_case"                    = "s3://${var.domain_bucket}/Data/Salesforce_Case"
    "--s3_destination_account"                 = "s3://${var.domain_bucket}/Data/Salesforce_Account"
    "--raw_table_case_comment"                 = "${var.raw_salesforce_case_comment_table}"
    "--s3_destination_case_comment"            = "s3://${var.domain_bucket}/Data/Salesforce_CaseComment"
    "--raw_table_user"                         = "${var.raw_salesforce_user_table}"
    "--s3_destination_user"                    = "s3://${var.domain_bucket}/Data/Salesforce_User"
    "--raw_table_record_type"                  = "${var.raw_salesforce_record_type_table}"
    "--s3_destination_record_type"             = "s3://${var.domain_bucket}/Data/Salesforce_RecordType"
    "--raw_table_contact"                      = "${var.raw_salesforce_contact_table}"
    "--s3_destination_contact"                 = "s3://${var.domain_bucket}/Data/Salesforce_Contact"
    "--raw_table_topic"                        = "${var.raw_salesforce_topic_table}"
    "--s3_destination_topic"                   = "s3://${var.domain_bucket}/Data/Salesforce_Topic"
    "--raw_table_topic_assignment"             = "${var.raw_salesforce_topic_assignment_table}"
    "--s3_destination_topic_assignment"        = "s3://${var.domain_bucket}/Data/Salesforce_TopicAssignment"
    "--raw_table_knowledgearticleversion"      = "${var.raw_salesforce_knowledge_article_version_table}"
    "--s3_destination_knowledgearticleversion" = "s3://${var.domain_bucket}/Data/Salesforce_KnowledgeArticleVersion"
    "--raw_table_salesforce_webactivity"       = "${var.raw_salesforce_webactivity_table}"
    "--s3_destination_salesforce_webactivity"  = "s3://${var.domain_bucket}/Data/Salesforce_WebActivity"
    "--raw_table_survey"                       = "${var.raw_salesforce_survey_table}"
    "--s3_destination_survey"                  = "s3://${var.domain_bucket}/Data/Salesforce_Survey"
    "--s3_destination_supportcases_salesforce" = "s3://${var.domain_bucket}/Data/SupportCases_Salesforce"
    "--job-bookmark-option"                    = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "BigQuery_Community_Since201901" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Salesforce/domain.Load_BigQuery_Community_Since201901.py"
  source = "scripts/Salesforce/domain.Load_BigQuery_Community_Since201901.py"
  etag   = "${md5(file("scripts/Salesforce/domain.Load_BigQuery_Community_Since201901.py"))}"
}

resource "aws_glue_job" "Load_BigQuery_Community_Since201901" {
  name               = "domain.Load_BigQuery_Community_Since201901"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.BigQuery_Community_Since201901.id}"
  }

  default_arguments = {
    "--raw_db"                                 = "${var.raw_database}"
    "--domain_db"                              = "${var.domain_database}"
    "--raw_table_bigquery"                     = "${var.raw_bigquery_table}"
    "--domain_table_salesforce_user"           = "${var.domain_Salesforce_user_table}"
    "--domain_table_users"                     = "${var.domain_users_table}"
    "--s3_destination_bigquery"                = "s3://${var.domain_bucket}/Data/BigQuery_Community_Since201901"
    "--job-bookmark-option"                    = "${var.bookmark}"
  }
}

resource "aws_glue_trigger" "salesforce_trigger" {
  name     = "trigger_salesforce_9_15_utc"
  type     = "SCHEDULED"
  schedule = "cron(0 9,15 ? * MON-FRI *)"
  enabled  = "${var.environment == "prod" ? true : false }"

  actions {
    job_name = "${aws_glue_job.Load_Salesforce_Transformations.name}"
  }
}

resource "aws_s3_bucket_object" "Utils" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/Salesforce/utils.py"
  source = "scripts/utils.py"
  etag   = "${md5(file("scripts/utils.py"))}"
}
