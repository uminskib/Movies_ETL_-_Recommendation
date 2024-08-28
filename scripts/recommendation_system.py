from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover, Tokenizer, HashingTF, IDF
from pyspark.sql.functions import col, udf, concat_ws, rank, desc, lower, regexp_replace
from pyspark.sql.types import FloatType
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
from datetime import datetime
import sys


def create_recommendation_system(project_id, bucket_name, dataset_id, table_id):
    # Start Spark session
    spark = SparkSession.builder.appName(
        "Content-Based Recommendation system"
    ).getOrCreate()

    # Load processed data from the data warehouse
    processed_movies_data = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}:{dataset_id}.{table_id}")
        .load()
    ).cache()

    # Get top 2500 most popular movies to analysis
    top_2500_most_popular_movie = processed_movies_data.orderBy(
        desc("popularity")
    ).limit(2500)

    # Select relevant columns
    movies_text_data = top_2500_most_popular_movie.select(
        "id", "title", "overview", "genre_names"
    )
    # Create 'content' column with lowercase conversion and punctuation removal
    movies_text_data = movies_text_data.withColumn(
        "content",
        lower(  # Convert to lowercase
            regexp_replace(  # Remove punctuation
                concat_ws(" ", col("title"), col("overview"), col("genre_names")),
                "[^\\w\\s]",  # This regex matches any character that is not a word character or whitespace
                "",  # Replace matched characters with empty string
            )
        ),
    )
    # Preprocess text data
    tokenizer = Tokenizer(inputCol="content", outputCol="words")

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")

    hashingTF = HashingTF(
        inputCol="filtered", outputCol="rawFeatures", numFeatures=2048
    )

    idf = IDF(inputCol="rawFeatures", outputCol="features")

    # Create pipeline and fit the model
    pipeline = [tokenizer, remover, hashingTF, idf]

    pipeline = Pipeline(stages=pipeline)
    model = pipeline.fit(movies_text_data)
    # Transform the data
    transformed_df = model.transform(movies_text_data).cache()

    # Define a UDF to calculate cosine similarity using vector functions
    def cosine_similarity_vectorized(v1, v2):
        return float(v1.dot(v2) / (v1.norm(2) * v2.norm(2)))

    cosine_similarity_udf = udf(cosine_similarity_vectorized, FloatType())

    # Cross Join for similarity calculation
    cross_df = (
        transformed_df.alias("df1")
        .join(transformed_df.alias("df2"), col("df1.id") < col("df2.id"))
        .select(
            col("df1.id").alias("df1_id"),
            col("df1.features").alias("df1_features"),
            col("df2.id").alias("df2_id"),
            col("df2.features").alias("df2_features"),
        )
    ).cache()

    # Calculate cosine similarity and drop na and keep only records with similarity > 0
    similarity_df = (
        cross_df.withColumn(
            "similarity", cosine_similarity_udf("df1_features", "df2_features")
        )
        .na.drop()
        .where(col("similarity") > 0)
    )

    # Window function for efficient top-N selection
    w = Window.partitionBy("df1_id").orderBy(desc("similarity"))

    # Get the top 3 similar movies
    top_3_similar = (
        similarity_df.withColumn("rank", rank().over(w))
        .filter(col("rank") <= 3)
        .select(
            col("df1_id").alias("id"),
            col("df2_id").alias("recommended_movie_id"),
            col("similarity"),
        )
    )

    # # Save the top 3 recommendations to Google Cloud Storage
    top_3_similar.write.parquet(
        f"gs://{bucket_name}//analysis//movie_recommendations_top3_{datetime.now().strftime('%Y%m%d')}.parquet",
        mode="overwrite",
    )

    spark.stop()


if __name__ == "__main__":
    # Extract arguments passed from SparkSubmitOperator from dag
    project_id = sys.argv[1] if len(sys.argv) > 1 else None
    bucket_name = sys.argv[2] if len(sys.argv) > 1 else None
    dataset_id = sys.argv[3] if len(sys.argv) > 1 else None
    table_id = sys.argv[4] if len(sys.argv) > 1 else None
    create_recommendation_system(project_id, bucket_name, dataset_id, table_id)
