from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import when, col, lit

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
            "table_stg",
            "namespace_stg",
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


def write_output_table(spark, df, namespace, table_name):
    df.createOrReplaceTempView("curated_view")
    spark.sql(
        f"""
        INSERT OVERWRITE {namespace}.{table_name}
        SELECT * FROM curated_view
    """
    )


def read_table_stg(spark, namespace, table):
    df_stg = spark.sql(f"SELECT * FROM {namespace}.{table}")
    return df_stg


def processar_dim_hostpital(spark, args):
    df = read_table_stg(spark, args["namespace_stg"], "hospitais")
    dim_hospital = df.select("id_hospital", "nome", "cidade", "estado")
    return dim_hospital


def processar_dim_profissional(spark, args):
    df = read_table_stg(spark, args["namespace_stg"], "profissionais_saude")
    dim_profissional = df.select("id_profissional", "nome", "especialidade", "crm")

    return dim_profissional


def processar_fato_consultas_procedimentos(spark, args):
    df_consultas = read_table_stg(spark, args["namespace_stg"], "consultas")
    df_procedimentos = read_table_stg(spark, args["namespace_stg"], "procedimentos")

    consultas = df_consultas.select(
        col("id_consulta").alias("id_fato"),
        col("id_paciente"),
        col("id_profissional"),
        col("data_consulta").alias("data_evento"),
        lit("Consulta").alias("tipo_evento"),
        col("diagnostico"),
        lit(None).cast(StringType()).alias("procedimento"),
        lit(None).cast(DoubleType()).alias("custo_estimado"),
        lit(None).cast(StringType()).alias("id_hospital"),
    )

    procedimentos = df_procedimentos.select(
        col("id_procedimento").alias("id_fato"),
        col("id_paciente"),
        lit(None).cast(StringType()).alias("id_profissional"),
        col("data_procedimento").alias("data_evento"),
        lit("Procedimento").alias("tipo_evento"),
        lit(None).cast(StringType()).alias("diagnostico"),
        col("tipo").alias("procedimento"),
        lit(None).cast(DoubleType()).alias("custo_estimado"),
        col("hospital_id").alias("id_hospital"),
    )

    fato_consultas_procedimentos = consultas.union(procedimentos)

    return fato_consultas_procedimentos



def processar_fato_exames_receitas(spark, args):
    df_consultas = read_table_stg(spark, args["namespace_stg"], "consultas")
    df_exames = read_table_stg(spark, args["namespace_stg"], "exames")
    df_pacientes = read_table_stg(spark, args["namespace_stg"], "internacoes")
    df_hospitais = read_table_stg(spark, args["namespace_stg"], "hospitais")
    df_receitas = read_table_stg(spark, args["namespace_stg"], "receitas")
    df_profissionais = read_table_stg(
        spark, args["namespace_stg"], "profissionais_saude"
    )

    # Adiciona coluna de custo ao exame
    df_exames = df_exames.withColumn(
        "custo_exame",
        when(col("tipo") == "sangue", lit(100.0))
        .when(col("tipo") == "imagem", lit(300.0))
        .otherwise(lit(150.0)),
    )

    # Adiciona coluna de custo à receita (exemplo simples: todas com custo fixo)
    df_receitas = df_receitas.withColumn("custo_receita", lit(50.0))

    # Une os exames com as consultas
    exames_completo = (
        df_exames.join(df_consultas, "id_consulta", "left")
        .join(df_pacientes, "id_paciente", "left")
        .join(df_profissionais, "id_profissional", "left")
        .join(df_hospitais, "id_hospital", "left")
    )

    # Une as receitas com as consultas
    receitas_completo = (
        df_receitas.join(df_consultas, "id_consulta", "left")
        .join(df_pacientes, "id_paciente", "left")
        .join(df_profissionais, "id_profissional", "left")
        .join(df_hospitais, "id_hospital", "left")
    )

    # Cria colunas comuns para unir
    exames_completo = exames_completo.selectExpr(
        "id_exame as id_evento",
        "id_consulta",
        "id_paciente",
        "id_profissional",
        "id_hospital",
        "tipo as tipo_evento",
        "resultado as descricao",
        "data",
        "custo_exame as custo",
    )

    receitas_completo = receitas_completo.selectExpr(
        "id_receita as id_evento",
        "id_consulta",
        "id_paciente",
        "id_profissional",
        "id_hospital",
        "medicamento as tipo_evento",
        "dosagem as descricao",
        "inicio as data",
        "custo_receita as custo",
    )

    # Junta exames e receitas na mesma fato
    df_fato_exames_receitas = exames_completo.unionByName(receitas_completo)

    return df_fato_exames_receitas


def main():
    args = get_args()
    tabela_destino = args["output_table"]

    spark = create_spark_session(args["s3_tables_bucket_arn"])

    if tabela_destino == "dim_hospital":
        df_final = processar_dim_hostpital(spark, args)
    elif tabela_destino == "dim_profissional":
        df_final = processar_dim_profissional(spark, args)
    elif tabela_destino == "fato_consultas_procedimentos":
        df_final = processar_fato_consultas_procedimentos(spark, args)
    elif tabela_destino == "fato_exames_receitas":
        df_final = processar_fato_exames_receitas(spark, args)

    create_namespace_if_not_exists(spark, args["output_namespace"])
    create_output_table_if_not_exists(
        spark, df_final, args["output_namespace"], args["output_table"]
    )
    write_output_table(spark, df_final, args["output_namespace"], args["output_table"])


if __name__ == "__main__":
    main()
