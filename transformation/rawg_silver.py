"""Transforma os JSONL da lista de jogos da RAWG em duas tabelas Parquet."""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


BRONZE_SCHEMA = """
    id BIGINT,
    name STRING,
    slug STRING,
    released STRING,
    updated STRING,
    _ingest_at STRING,
    _ingested_at STRING,
    rating DOUBLE,
    rating_top BIGINT,
    ratings_count BIGINT,
    reviews_count BIGINT,
    metacritic BIGINT,
    added BIGINT,
    playtime BIGINT,
    added_by_status STRUCT<
        beaten: BIGINT, dropped: BIGINT, owned: BIGINT,
        playing: BIGINT, toplay: BIGINT, yet: BIGINT
    >,
    ratings ARRAY<STRUCT<title: STRING, count: BIGINT>>,
    tags ARRAY<STRUCT<slug: STRING>>,
    genres ARRAY<STRUCT<id: BIGINT, name: STRING, slug: STRING>>
"""
STATUSES = ("beaten", "dropped", "owned", "playing", "toplay", "yet")
RATING_CATEGORIES = ("exceptional", "recommended", "meh", "skip")


def contar_avaliacoes(categoria):
    avaliacoes = F.filter("ratings", lambda item: F.lower(item["title"]) == categoria)
    total = F.aggregate(
        avaliacoes,
        F.lit(0).cast("long"),
        lambda soma, item: soma + F.coalesce(item["count"], F.lit(0)),
    )
    return F.coalesce(total, F.lit(0)).alias(f"ratings_{categoria}")


def montar_tabelas(bronze):
    """Prepara uma linha por jogo e dia e usa essa base nas duas tabelas."""
    base = (
        bronze.withColumn(
            "ingested_at",
            F.coalesce(F.try_to_timestamp("_ingested_at"), F.try_to_timestamp("_ingest_at")),
        )
        .withColumn("updated_at", F.try_to_timestamp("updated"))
        .withColumn("dt", F.to_date("ingested_at"))
    )

    if base.filter("id IS NULL OR dt IS NULL").limit(1).count():
        raise ValueError("A entrada tem registros sem ID ou data de ingestão válida.")

    por_jogo_e_dia = Window.partitionBy("id", "dt").orderBy(F.col("ingested_at").desc())
    jogos_unicos = (
        base.withColumn("ordem", F.row_number().over(por_jogo_e_dia))
        .filter(F.col("ordem") == 1)
        .drop("ordem")
    )

    status_cols = [
        F.coalesce(F.col(f"added_by_status.{status}"), F.lit(0)).alias(f"status_{status}")
        for status in STATUSES
    ]
    release_date = F.try_to_timestamp("released").cast("date")
    free_to_play_tag = F.exists("tags", lambda tag: tag["slug"] == "free-to-play")

    games = jogos_unicos.select(
        F.col("id").alias("game_id"),
        "name", "slug", "dt",
        release_date.alias("release_date"),
        F.year(release_date).alias("release_year"),
        "rating", "rating_top", "ratings_count", "reviews_count",
        "metacritic", "added", "playtime",
        *status_cols,
        *[contar_avaliacoes(categoria) for categoria in RATING_CATEGORIES],
        F.coalesce(free_to_play_tag, F.lit(False)).alias("is_free_to_play"),
        "updated_at", "ingested_at",
    )

    game_genres = (
        jogos_unicos.select(F.col("id").alias("game_id"), "dt", F.explode("genres").alias("genre"))
        .select(
            "game_id", "dt",
            F.col("genre.id").alias("genre_id"),
            F.col("genre.name").alias("genre_name"),
            F.col("genre.slug").alias("genre_slug"),
        )
        .filter("genre_id IS NOT NULL")
        .dropDuplicates(["game_id", "dt", "genre_id"])
    )
    return games, game_genres


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Arquivo ou diretório de JSONL da lista RAWG")
    parser.add_argument("--output", required=True, help="Diretório de saída dos Parquets locais")
    args, _ = parser.parse_known_args()

    spark = (
        SparkSession.builder.appName("rawg-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        bronze = spark.read.schema(BRONZE_SCHEMA).json(args.input)
        games, game_genres = montar_tabelas(bronze)

        for nome, tabela in (("games", games), ("game_genres", game_genres)):
            destino = f"{args.output.rstrip('/')}/{nome}"
            tabela.write.mode("overwrite").partitionBy("dt").parquet(destino)
            total = spark.read.parquet(destino).count()
            print(f"{nome}: {total} linhas em {destino}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()