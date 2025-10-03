from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType
from pyspark.sql.functions import to_date, col

from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
import sys


def get_args():
    return getResolvedOptions(
        sys.argv,
        [
            "output_table",
            "s3_tables_bucket_arn",
            "output_namespace",
            "table",
            "s3_path_raw",
            "primary_keys",
        ],
    )


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


def create_namespace_if_not_exists(spark, namespace):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.{namespace}")


def create_output_table_if_not_exists(spark, df, namespace, table_name):
    ddl = ", ".join([f"{c} {t}" for c, t in df.dtypes])
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {namespace}.{table_name} (
            {ddl}
        )
    """
    )


def write_output_table(spark, df, namespace, table_name, primary_key):
    df.createOrReplaceTempView("source_view")

    columns = df.columns

    pk_cols = primary_key.split(",")
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_cols])
    set_clause = ", ".join([f"{col} = source.{col}" for col in columns])

    spark.sql(f"""
        MERGE INTO {namespace}.{table_name} AS target
        USING source_view AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT *
    """)

def read_s3_raw(spark, s3_path_raw, table):
    #df = spark.read.csv(f"{s3_path_raw}{table}.csv", header=True, inferSchema=True)
    df = spark.read.parquet(f"{s3_path_raw}{table}/")
    return df


def processar_hospitais(df):
    df_staged = df.select(
        df["id_hospital"].cast(IntegerType()).alias("id_hospital"),
        df["nome"].cast(StringType()).alias("nome"),
        df["cidade"].cast(StringType()).alias("cidade"),
        df["estado"].cast(StringType()).alias("estado"),
    )

    return df_staged


def processar_profissionais_saude(df):

    df_staged = df.select(
        df["id_profissional"].cast(IntegerType()).alias("id_profissional"),
        df["nome"].cast(StringType()).alias("nome"),
        df["especialidade"].cast(StringType()).alias("especialidade"),
        df["crm"].cast(StringType()).alias("crm"),
        df["hospital_id"].cast(IntegerType()).alias("hospital_id"),
    )

    return df_staged


def processar_consultas(df):
    df_staged = df.select(
        df["id_consulta"].cast(IntegerType()).alias("id_consulta"),
        df["id_paciente"].cast(IntegerType()).alias("id_paciente"),
        df["id_profissional"].cast(IntegerType()).alias("id_profissional"),
        df["data_consulta"].cast(DateType()).alias("data_consulta"),
        df["motivo"].cast(StringType()).alias("motivo"),
        df["diagnostico"].cast(StringType()).alias("diagnostico"),
    )

    df_staged = df_staged.withColumn(
        "data_consulta", to_date(col("data_consulta"), "yyyy-MM-dd")
    )

    return df_staged


def processar_exames(df):
    df_staged = df.select(
        df["id_exame"].cast(IntegerType()).alias("id_exame"),
        df["id_consulta"].cast(IntegerType()).alias("id_consulta"),
        df["tipo"].cast(StringType()).alias("tipo"),
        df["resultado"].cast(StringType()).alias("resultado"),
        df["data"].cast(DateType()).alias("data"),
    )

    df_staged = df_staged.withColumn("data", to_date(col("data"), "yyyy-MM-dd"))

    return df_staged


def processar_receitas(df):
    df_staged = (
        df.withColumn("id_receita", col("id_receita").cast(IntegerType()))
        .withColumn("id_consulta", col("id_consulta").cast(IntegerType()))
        .withColumn("medicamento", col("medicamento").cast(StringType()))
        .withColumn("dosagem", col("dosagem").cast(StringType()))
        .withColumn("inicio", to_date(col("inicio"), "yyyy-MM-dd"))
        .withColumn("fim", to_date(col("fim"), "yyyy-MM-dd"))
    )

    df_staged = df_staged.withColumn(
        "inicio", to_date(col("inicio"), "yyyy-MM-dd")
    ).withColumn("fim", to_date(col("fim"), "yyyy-MM-dd"))

    return df_staged


def processar_internacoes(df):
    df_staged = (
        df.withColumn("id_internacao", col("id_internacao").cast(IntegerType()))
        .withColumn("id_paciente", col("id_paciente").cast(IntegerType()))
        .withColumn("id_hospital", col("id_hospital").cast(IntegerType()))
        .withColumn("motivo", col("motivo").cast(StringType()))
        .withColumn("data_entrada", to_date(col("data_entrada"), "yyyy-MM-dd"))
        .withColumn("data_saida", to_date(col("data_saida"), "yyyy-MM-dd"))
    )

    df_staged = df_staged.withColumn(
        "data_entrada", to_date(col("data_entrada"), "yyyy-MM-dd")
    ).withColumn("data_saida", to_date(col("data_saida"), "yyyy-MM-dd"))

    return df_staged


def processar_procedimentos(df):
    df_staged = (
        df.withColumn("id_procedimento", col("id_procedimento").cast(IntegerType()))
        .withColumn("id_paciente", col("id_paciente").cast(IntegerType()))
        .withColumn("tipo", col("tipo").cast(StringType()))
        .withColumn(
            "data_procedimento", to_date(col("data_procedimento"), "yyyy-MM-dd")
        )
        .withColumn("hospital_id", col("hospital_id").cast(IntegerType()))
    )

    df_staged = df_staged.withColumn(
        "data_procedimento", to_date(col("data_procedimento"), "yyyy-MM-dd")
    )

    return df_staged


def main():
    args = get_args()
    tabela_destino = args["table"]

    spark = create_spark_session(args["s3_tables_bucket_arn"])

    df = read_s3_raw(spark, args["s3_path_raw"], tabela_destino)

    if tabela_destino == "hospitais":
        df_final = processar_hospitais(df)
    elif tabela_destino == "profissionais_saude":
        df_final = processar_profissionais_saude(df)
    elif tabela_destino == "consultas":
        df_final = processar_consultas(df)
    elif tabela_destino == "exames":
        df_final = processar_exames(df)
    elif tabela_destino == "receitas":
        df_final = processar_receitas(df)
    elif tabela_destino == "internacoes":
        df_final = processar_internacoes(df)
    elif tabela_destino == "procedimentos":
        df_final = processar_procedimentos(df)

    create_namespace_if_not_exists(spark, args["output_namespace"])
    create_output_table_if_not_exists(
        spark, df_final, args["output_namespace"], args["output_table"]
    )
    write_output_table(spark, df_final, args["output_namespace"], args["output_table"], args["primary_keys"])


if __name__ == "__main__":
    main()
