from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    to_date,
    udf,
)
from pyspark.sql.types import StringType
from datetime import datetime
import sys


def transform_movie_data(gcs_path):
    spark = SparkSession.builder.appName(
        "Movie_transform_task_with_gcp_bucket"
    ).getOrCreate()
    # Read all JSON files (with wildcard)
    movies_df = spark.read.json(gcs_path + "//raw//trending_movies_*.json")
    genres_df = spark.read.json(gcs_path + "//raw//movie_genres_*.json")

    movies_df = movies_df.drop("poster_path", "media_type", "adult", "backdrop_path")

    # Extract extract_datetime from filenames in movies_df
    movies_df = movies_df.withColumn(
        "extract_date", regexp_extract(input_file_name(), r"([0-9]+)", 1)
    )
    movies_df = movies_df.withColumn(
        "extract_date", to_date(movies_df.extract_date, "yyyyMMdd")
    )
    movies_df = movies_df.withColumn(
        "release_date", to_date(movies_df.release_date, "yyyy-MM-dd")
    )

    # Extract extract_datetime from filenames in genres_df
    genres_df = genres_df.withColumn(
        "extract_date", regexp_extract(input_file_name(), r"([0-9]+)", 1)
    )
    genres_df = genres_df.withColumn(
        "extract_date", to_date(genres_df.extract_date, "yyyyMMdd")
    )

    # Keep unique movie records based on 'id' and latest extract_datetime
    movies_df = movies_df.dropDuplicates(["id"]).orderBy(col("extract_date").desc())

    genres_df = genres_df.dropDuplicates(["id"]).orderBy(col("extract_date").desc())

    # Create a dictionary from the genre_dict_df
    genre_dict = {row.id: row.name for row in genres_df.drop("extract_date").collect()}

    # Define a UDF to map genre IDs to names
    def map_genres(genre_ids):
        if genre_ids is None:
            return []
        return [genre_dict.get(genre_id) for genre_id in genre_ids]

    map_genres_udf = udf(map_genres, StringType())

    # Apply the UDF to the data DataFrame
    movies_df = movies_df.withColumn("genre_names", map_genres_udf(col("genre_ids")))
    movies_df = movies_df.drop("genre_ids")

    join_udf = udf(lambda x: ", ".join(x))

    movies_df = movies_df.withColumn("genre_names", join_udf(col("genre_names")))

    # Save transformed data to GCS
    movies_df.write.parquet(
        f"{gcs_path}//transformed//trending_movies_{datetime.now().strftime('%Y%m%d')}.parquet",
        mode="overwrite",
    )
    genres_df.write.parquet(
        f"{gcs_path}//transformed//movie_genres_{datetime.now().strftime('%Y%m%d')}.parquet",
        mode="overwrite",
    )
    spark.stop()


if __name__ == "__main__":
    # Extract arguments passed from SparkSubmitOperator
    gcs_path = sys.argv[1] if len(sys.argv) > 1 else None

    transform_movie_data(gcs_path)
