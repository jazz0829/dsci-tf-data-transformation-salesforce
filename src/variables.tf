variable "region" {
  default = "eu-west-1"
}

variable "tags_team" {
  default = "cig"
}

variable "app_name" {
  default = "cig-tf-data-transformation-salesforce"
}

variable "environment" {}

variable "project_name" {
  default = "data-transformation-salesforce"
}

variable "glue_artifact_store" {
  default = "cig-build-artifact-store"
}

variable "bookmark" {
  default = "job-bookmark-disable"
}

variable "dpu" {
  default = 5
}

variable "domain_bucket" {}

variable "raw_bucket" {}

variable "domain_database"{
  default = "customerintelligence"
}

variable "raw_database" {
  default = "customerintelligence_raw"
}

variable "raw_salesforce_case_table" {
  default = "object_case"
}

variable "raw_salesforce_account_table" {
  default = "object_account"
}

variable "raw_salesforce_webactivity_table" {
  default = "object_webactivity"
}

variable "raw_salesforce_case_comment_table" {
  default = "object_casecomment"
}

variable "raw_salesforce_user_table" {
  default = "object_user"
}

variable "raw_salesforce_record_type_table" {
  default = "object_recordtype"
}

variable "raw_salesforce_contact_table" {
  default = "object_contact"
}

variable "raw_salesforce_topic_table" {
  default = "object_topic"
}

variable "raw_salesforce_topic_assignment_table" {
  default = "object_topicassignment"
}

variable "raw_salesforce_knowledge_article_version_table" {
  default = "object_knowledgearticleversion"
}

variable "raw_salesforce_survey_table" {
  default = "object_survey"
}

variable "raw_bigquery_table" {
  default = "source_bigquery"
}

variable "domain_Salesforce_user_table" {
  default = "Salesforce_user"
}

variable "domain_users_table" {
  default = "users"
}