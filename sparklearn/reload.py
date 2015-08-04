
import sys
from random import random
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

from pyspark.sql.functions import UserDefinedFunction

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType




if __name__ == "__main__":

    sc = SparkContext(appName="Parquet")
    sqlContext = SQLContext(sc)

    #  Reload the parquet file to get back the dataframe
    # df = sqlContext.read.load("namesAndAges.parquet", format="parquet")
    df = sqlContext.read.load("HungamaSimilarityIntermediate.parquet", format="parquet")

    df.show()

