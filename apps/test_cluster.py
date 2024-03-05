import os
import random
import string
from pyspark.sql import SparkSession

# Count words using PySpark
def count_words(filename):
    spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .getOrCreate()
    
    lines = spark.read.text(filename).rdd.map(lambda r: r[0])
    word_counts = lines.flatMap(lambda line: line.split(" ")) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)
    
    word_counts.saveAsTextFile("/opt/spark-data/result.txt")
    
    spark.stop()

if __name__ == "__main__":
    filename = "random_text.txt"
    count_words(f'/opt/spark-data/{filename}')
