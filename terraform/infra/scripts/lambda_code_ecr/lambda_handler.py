import json
import duckdb
import os
import logging
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def lambda_handler(event, context):

    def query_duck_db(event):
        try:
            catalog_arn = "arn:aws:s3tables:us-east-2:296735965303:bucket/datahandon-genai-hospital"
            query = event["requestBody"]["content"]["application/json"]["properties"][
                0
            ]["value"]

            # Create a temporary directory for DuckDB home
            temp_dir = tempfile.mkdtemp()
            logger.info(f"Created temporary directory for DuckDB home: {temp_dir}")

            # Initialize DuckDB with explicit home directory
            conn = duckdb.connect(":memory:")
            conn.execute(f"SET home_directory='{temp_dir}'")
            logger.info(
                "DuckDB connection initialized successfully with custom home directory"
            )

            # Install and load required extensions
            extensions = ["aws", "httpfs", "iceberg", "parquet", "avro"]
            for ext in extensions:
                conn.execute(f"INSTALL {ext};")
                conn.execute(f"LOAD {ext};")
                logger.info(f"Installed and loaded {ext} extension")

            # Force install iceberg from core_nightly
            conn.execute("FORCE INSTALL iceberg FROM core_nightly;")
            conn.execute("LOAD iceberg;")
            logger.info("Forced installation and loaded iceberg from core_nightly")

            # Set up AWS credentials
            conn.execute("CALL load_aws_credentials();")
            conn.execute(
                """
            CREATE SECRET (
                TYPE s3,
                PROVIDER credential_chain
            );
            """
            )
            logger.info("Set up AWS credentials")

            # Attach S3 Tables catalog using ARN
            try:
                conn.execute(
                    f"""
                ATTACH '{catalog_arn}' 
                AS s3_tables_db (
                    TYPE iceberg,
                    ENDPOINT_TYPE s3_tables
                );
                """
                )
                logger.info(f"Successfully attached S3 Tables catalog: {catalog_arn}")
            except Exception as e:
                logger.error(f"Catalog attachment failed: {str(e)}")
                return {
                    "statusCode": 500,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(
                        {"error": "Catalog connection failed", "details": str(e)}
                    ),
                }

            # Execute query with enhanced error handling
            try:
                result = conn.execute(query).fetchall()
                columns = [desc[0] for desc in conn.description]

                formatted = [dict(zip(columns, row)) for row in result]
                logger.info(
                    f"Query executed successfully. Returned {len(formatted)} rows."
                )
                response_body = json.dumps(formatted)

                return response_body

            except Exception as query_error:
                logger.error(f"Query execution failed: {str(query_error)}")
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(
                        {
                            "error": "Query execution error",
                            "details": str(query_error),
                            "suggestions": [
                                "Verify table exists in the attached database",
                                "Check your S3 Tables ARN format",
                                "Validate AWS permissions for the bucket",
                            ],
                        }
                    ),
                }

        except Exception as e:
            logger.error(f"Global error: {str(e)}")
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(
                    {"error": "Unexpected runtime error", "details": str(e)}
                ),
            }
        finally:
            if "conn" in locals():
                conn.close()

    action_group = event.get("actionGroup")
    api_path = event.get("apiPath")

    print("api_path: ", api_path)

    result = ""
    response_code = 200

    if api_path == "/duckQuery":
        result = query_duck_db(event)
    else:
        response_code = 404
        result = {"error": f"Unrecognized api path: {action_group}::{api_path}"}

    response_body = {"application/json": {"body": result}}

    action_response = {
        "actionGroup": action_group,
        "apiPath": api_path,
        "httpMethod": event.get("httpMethod"),
        "httpStatusCode": response_code,
        "responseBody": response_body,
    }

    api_response = {"messageVersion": "1.0", "response": action_response}
    return api_response
