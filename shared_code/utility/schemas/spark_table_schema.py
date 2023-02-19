from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType

image_table_schema = StructType([
	StructField("PartitionKey", StringType(), True),
	StructField("RowKey", StringType(), True),
	StructField("image", StringType(), True),
	StructField("text", StringType(), True),
	StructField("id", StringType(), True),
	StructField("author", StringType(), True),
	StructField("url", StringType(), True),
	StructField("flair", StringType(), True),
	StructField("permalink", StringType(), True),
	StructField("hash", StringType(), True),
	StructField("subreddit", StringType(), True),
	StructField("caption", StringType(), True),
	StructField("exists", BooleanType(), True),
	StructField("image_name", StringType(), True),
	StructField("updated_caption", StringType(), True),
	StructField("small_image", StringType(), True),
	StructField("curated", BooleanType(), True)
])

tokenize_caption_schema = ArrayType(StructType([
    StructField("Type", StringType(), True),
    StructField("Value", StringType(), True)
]))

image_table_schema_with_caption_tokens = StructType([
	StructField("PartitionKey", StringType(), True),
	StructField("RowKey", StringType(), True),
	StructField("image", StringType(), True),
	StructField("text", StringType(), True),
	StructField("id", StringType(), True),
	StructField("author", StringType(), True),
	StructField("url", StringType(), True),
	StructField("flair", StringType(), True),
	StructField("permalink", StringType(), True),
	StructField("hash", StringType(), True),
	StructField("subreddit", StringType(), True),
	StructField("caption", StringType(), True),
	StructField("exists", BooleanType(), True),
	StructField("image_name", StringType(), True),
	StructField("updated_caption", StringType(), True),
	StructField("small_image", StringType(), True),
	StructField("curated", BooleanType(), True),
	StructField("caption_tokens", tokenize_caption_schema, True),
	StructField("text_tokens", tokenize_caption_schema, True)
])
