from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
import sys

RULESETS = {
    "fato_consultas_procedimentos": """
        Rules = [
            ColumnExists "id_fato",
            ColumnExists "id_paciente",
            ColumnExists "id_profissional",
            ColumnExists "data_evento",
            ColumnExists "tipo_evento",
            ColumnExists "diagnostico",
            ColumnExists "procedimento",
            ColumnExists "custo_estimado",
            ColumnExists "id_hospital",

            IsComplete "id_fato",
            IsComplete "id_paciente",
            IsComplete "id_profissional",
            IsComplete "data_evento",
            IsComplete "procedimento",

            ColumnDataType "id_fato" = "int",
            ColumnDataType "id_paciente" = "int",
            ColumnDataType "id_profissional" = "string",
            ColumnDataType "data_evento" = "date",
            ColumnDataType "tipo_evento" = "string",
            ColumnDataType "diagnostico" = "string",
            ColumnDataType "procedimento" = "string",
            ColumnDataType "custo_estimado" = "double",
            ColumnDataType "id_hospital" = "string",

            RowCount > 0,

            CustomSql "SELECT id_fato FROM primary GROUP BY id_fato HAVING COUNT(*) > 1",  # detecta duplicatas
            CustomSql "SELECT custo_estimado FROM primary WHERE custo_estimado < 0",
            CustomSql "SELECT data_evento FROM primary WHERE data_evento > current_date"
        ]
    """,
    "fato_exames_receitas": """
        Rules = [
            ColumnExists "id_evento",
            ColumnExists "id_consulta",
            ColumnExists "id_paciente",
            ColumnExists "id_profissional",
            ColumnExists "id_hospital",
            ColumnExists "tipo_evento",
            ColumnExists "descricao",
            ColumnExists "data",
            ColumnExists "custo",

            IsComplete "id_evento",
            IsComplete "id_consulta",
            IsComplete "id_paciente",
            IsComplete "tipo_evento",
            IsComplete "descricao",
            IsComplete "data",

            ColumnDataType "id_evento" = "int",
            ColumnDataType "id_consulta" = "int",
            ColumnDataType "id_paciente" = "int",
            ColumnDataType "id_profissional" = "int",
            ColumnDataType "id_hospital" = "int",
            ColumnDataType "tipo_evento" = "string",
            ColumnDataType "descricao" = "string",
            ColumnDataType "data" = "date",
            ColumnDataType "custo" = "double",

            RowCount > 0,

            CustomSql "SELECT id_evento FROM primary GROUP BY id_evento HAVING COUNT(*) > 1",  # detecta duplicatas
            CustomSql "SELECT custo FROM primary WHERE custo < 0",
            CustomSql "SELECT data FROM primary WHERE data > current_date"
        ]
    """,
    "dim_hospital": """
        Rules = [
            ColumnExists "id_hospital",
            ColumnExists "nome",
            ColumnExists "cidade",
            ColumnExists "estado",

            IsComplete "id_hospital",
            IsComplete "nome",
            IsComplete "cidade",
            IsComplete "estado",

            ColumnDataType "id_hospital" = "int",
            ColumnDataType "nome" = "string",
            ColumnDataType "cidade" = "string",
            ColumnDataType "estado" = "string",

            RowCount > 0,

            CustomSql "SELECT id_hospital FROM primary GROUP BY id_hospital HAVING COUNT(*) > 1"
        ]
    """,
    "dim_profissional": """
       Rules = [
            ColumnExists "id_profissional",
            ColumnExists "nome",
            ColumnExists "especialidade",
            ColumnExists "crm",

            IsComplete "id_profissional",
            IsComplete "nome",
            IsComplete "crm",

            ColumnDataType "id_profissional" = "int",
            ColumnDataType "nome" = "string",
            ColumnDataType "especialidade" = "string",
            ColumnDataType "crm" = "string",

            RowCount > 0,

            CustomSql "SELECT id_profissional FROM primary GROUP BY id_profissional HAVING COUNT(*) > 1",
            CustomSql "SELECT crm FROM primary WHERE LENGTH(crm) < 4"  # validação simples do CRM
        ]

    """,
}


def create_spark_session(s3_warehouse_arn):
    return (
        SparkSession.builder.appName("glue-s3-tables-merge")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", "s3tablesbucket")
        .config(
            "spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.s3tablesbucket.catalog-impl",
            "software.amazon.s3tables.iceberg.S3TablesCatalog",
        )
        .config("spark.sql.catalog.s3tablesbucket.warehouse", s3_warehouse_arn)
        .getOrCreate()
    )


def ler_dimensoes(spark, namespace):
    return {
        "fato_exames_receitas": spark.sql(
            f"SELECT * FROM {namespace}.fato_exames_receitas"
        ),
        "fato_consultas_procedimentos": spark.sql(
            f"SELECT * FROM {namespace}.fato_consultas_procedimentos"
        ),
        "dim_profissional": spark.sql(
            f"SELECT * FROM {namespace}.dim_profissional"
        ),
        "dim_hospital": spark.sql(
            f"SELECT * FROM {namespace}.dim_hospital"
        ),
    }


def aplicar_regras_dq(df, glueContext, nome_contexto, ruleset):
    df_dq = DynamicFrame.fromDF(df, glueContext, nome_contexto)

    dqResultsEvaluator = EvaluateDataQuality().process_rows(
        frame=df_dq,
        ruleset=ruleset,
        publishing_options={
            "dataQualityEvaluationContext": nome_contexto,
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
            "resultsS3Prefix": "s3://cjmm-mds-lake-curated/genai_data_quality_results/glue_dq",
        },
        additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
    )

    ruleOutcomes = SelectFromCollection.apply(
        dfc=dqResultsEvaluator,
        key="ruleOutcomes",
        transformation_ctx="ruleOutcomes",
    )

    dqResults = ruleOutcomes.toDF()

    print(dqResults.printSchema())
    dqResults.show(truncate=False)


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "s3_tables_bucket_arn",
            "namespace",
        ],
    )
    spark = create_spark_session(args["s3_tables_bucket_arn"])
    glueContext = GlueContext(spark.sparkContext)

    tabelas = ler_dimensoes(spark, args["namespace"])

    for nome_tabela, df in tabelas.items():
        print(f"Aplicando regras de qualidade em: {nome_tabela}")
        aplicar_regras_dq(
            df,
            glueContext,
            f"dh_genai_hospital_{nome_tabela}",
            RULESETS[nome_tabela],
        )


if __name__ == "__main__":
    main()
