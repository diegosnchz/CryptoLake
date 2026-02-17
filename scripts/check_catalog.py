from src.processing.spark_session import build_spark_session


def main() -> None:
    spark = build_spark_session("CryptoLake-CheckCatalog")
    queries = [
        "SHOW NAMESPACES IN cryptolake",
        "SHOW TABLES IN cryptolake.gold",
        "SHOW TABLES IN cryptolake.silver",
        "SHOW TABLES IN gold",
        "SHOW TABLES IN silver",
    ]
    for query in queries:
        print(f"--- {query}")
        try:
            spark.sql(query).show(truncate=False)
        except Exception as exc:
            print(f"ERR: {exc}")

    print("--- SELECT count(*) FROM cryptolake.gold.fact_ohlcv_1m")
    try:
        spark.sql("SELECT count(*) AS c FROM cryptolake.gold.fact_ohlcv_1m").show()
    except Exception as exc:
        print(f"ERR: {exc}")

    spark.stop()


if __name__ == "__main__":
    main()
