#terraform apply -var-file=envs/develop.tfvars -auto-approve
#terraform init -backend-config="backends/develop.hcl"

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

###############################################################################
#########             VPC E SUBNETS                               #############
###############################################################################
module "vpc_public" {
  source                = "./modules/vpc"
  project_name          = "data-handson-mds"
  vpc_name              = "data-handson-mds-vpc-${var.environment}"
  vpc_cidr              = "10.0.0.0/16"
  public_subnet_cidrs   = ["10.0.1.0/24", "10.0.2.0/24"]
  private_subnet_cidrs  = ["10.0.3.0/24", "10.0.4.0/24"]
  availability_zones    = ["us-east-2a", "us-east-2b"]
}

###############################################################################
#########             RDS POSTGRESQL                              #############
###############################################################################
module "rds_postgres" {
  source = "./modules/rds"

  environment       = var.environment
  vpc_id            = module.vpc_public.vpc_id
  public_subnet_ids = module.vpc_public.public_subnet_ids
  
  db_name     = "postgres"
  db_username = "postgres"
  db_password = var.rds_password
  
  instance_class     = "db.t3.large"
  allocated_storage  = 50
}


##############################################################################
########             INSTANCIAS EC2                              #############
##############################################################################
# module "ec2_instance" {
#   source             =  "./modules/ec2"
#   ami_id              = "ami-04b4f1a9cf54c11d0"
#   instance_type       = "t3a.2xlarge"
#   subnet_id           = module.vpc_public.public_subnet_ids[0]
#   vpc_id              = module.vpc_public.vpc_id
#   key_name            = "cjmm-datahandson-cb"
#   associate_public_ip = true
#   instance_name       = "data-handson-mds-ec2-${var.environment}"
  
#   user_data = templatefile("${path.module}/scripts/bootstrap/ec2_bootstrap.sh", {})

#   ingress_rules = [
#     {
#       from_port   = 22
#       to_port     = 22
#       protocol    = "tcp"
#       cidr_blocks = ["0.0.0.0/0"]
#     },
#     {
#       from_port   = 80
#       to_port     = 80
#       protocol    = "tcp"
#       cidr_blocks = ["0.0.0.0/0"]
#     },
#     {
#       from_port   = 443
#       to_port     = 443
#       protocol    = "tcp"
#       cidr_blocks = ["0.0.0.0/0"]
#     }
#   ]
# }


###############################################################################
#########            GLUE JOBS                                   #############
###############################################################################
module "glue_jobs_gdq_s3tables" {
  source = "./modules/glue-job"

  project_name      = "datahandson-genai-glue-jobs"
  environment       = var.environment
  region            = var.region
  s3_bucket_scripts = var.s3_bucket_scripts
  s3_bucket_data    = var.s3_bucket_raw
  scripts_local_path = "scripts/glue_etl/"
  
  job_scripts = {
    "datahandson-genai-raw-staged-todas-tabelas" = "datahandson-genai-raw-staged-todas-tabelas.py",
    "datahandson-genai-staged-curated-todas-tabelas" = "datahandson-genai-staged-curated-todas-tabelas.py",
    "datahandson-genai-gdq-dims-facts" = "datahandson-genai-gdq-dims-facts.py"
  }
  
  worker_type       = "G.1X"
  number_of_workers = 3
  timeout           = 60
  max_retries       = 0
  
  extra_jars = "s3://cjmm-mds-lake-configs/jars/s3-tables-catalog-for-iceberg-0.1.7.jar"

  
  additional_arguments = {
    "--enable-glue-datacatalog" = "true"
    "--user-jars-first"         = "true"
    "--datalake-formats"        = "iceberg"
  }
}


###############################################################################
#########            LAMBDA FUNCTIONS                            #############
###############################################################################
module "lambda_query_athena_bedrock" {
  source = "./modules/lambda"

  project_name    = "datahandson-genai"
  environment     = var.environment
  function_name   = "datahandson-genai-bedrock-query-athena"
  description     = "Lambda function to Query in Athena with Bedrock Agent"
  handler         = "datahandson-genai-bedrock-query-athena.lambda_handler"
  runtime         = "python3.9"
  timeout         = 900
  memory_size     = 128
  source_code_file = "datahandson-genai-bedrock-query-athena.py"

}

###############################################################################
#########            LAMBDA FUNCTION WITH DOCKER                  #############
###############################################################################
module "lambda_function_duckdb" {
  source = "./modules/lambda_ecr"

  function_name = "datahandson-mds-s3tables-duckdb"
  description   = "Python Lambda function for querying S3 tables with DuckDB"
  
  # Docker image URI (replace with your actual ECR URI after running build_and_push.sh)
  image_uri     = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com/lambda-duckdb:latest"
  
  # Optional parameters
  timeout       = 900
  memory_size   = 2048
  ephemeral_storage_size = 2048
  
  # Function URL configuration
  create_function_url = true
  function_url_auth_type = "AWS_IAM"  # Use AWS IAM for authentication
  
  environment_variables = {
    ENV_VAR_1 = "value1"
  }
}


###############################################################################
#########            STEP FUNCTIONS                               #############
###############################################################################
module "step_functions" {
  source = "./modules/step-functions"

  project_name = "data-handson-genai"
  environment  = var.environment
  region       = var.region
  
  # Definições das máquinas de estado
  state_machines = {
    "datahandson-genai-s3tables-glueetl-hospital" = {
      definition_file = "sfn_definition_s3tables_hospitais.json"
      type            = "STANDARD"
    }
  }
  
  # Permissões adicionais para o Step Functions
  additional_iam_statements = [
    {
      Effect = "Allow"
      Action = [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun"
      ]
      Resource = "*"
    }
  ]
  
  # Anexar políticas gerenciadas
  attach_glue_policy = true
  
  # Configurações de logging
  log_retention_days = 30
  include_execution_data = true
  logging_level = "ALL"
}
