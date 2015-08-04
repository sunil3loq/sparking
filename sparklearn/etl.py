import sys
from random import random
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

from pyspark.sql.functions import UserDefinedFunction

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType



# If a song has been listened for less than 5 seconds then the user has not listened to the song
listenduration = 5

# If a song has been listened for less than 30 seconds then the user has listened to the song but not liked it
likeduration = 30


# --------------------------------------------------------------------------------------------
def likedSongs(duration):
    if duration > likeduration:
        return 1
    return 0


class HungamaDataAnalyzer:

    def __init__(self, inputFile, metaFile):

        self.transactionDataFile = inputFile
        self.metaDataFile = metaFile

        #  We are making a copy for self join... once such an issue is resolved we should be fine
        self.trans_dataFrame_x = None
        self.trans_dataFrame_y = None
        self.meta_dataFrame = None
        self.similarity_dataFrame = None


    # Read the input files and convert them to a dataframe
    def read_input(self, sc, sqlContext):

        # Parse the transaction dataset
        readTransLines = sc.textFile(self.transactionDataFile)

        # Filter out the first line as it is only the header
        transParts = readTransLines.map(lambda l: l.split(","))\
                                   .filter(lambda x: x[0] != 'msisdn')

        # Read specific rows in the dataset along with specifying the schema
        transDataset_x = transParts.map(lambda p: Row(msisdn_x=p[0], songcode_x=p[1], duration_x=int(p[2]), businesscategory_x=p[7]))
        transDataset_y = transParts.map(lambda p: Row(msisdn_y=p[0], songcode_y=p[1], duration_y=int(p[2]), businesscategory_y=p[7]))

        # Infer the dataframes
        self.trans_dataFrame_x = sqlContext.createDataFrame(transDataset_x)
        self.trans_dataFrame_y = sqlContext.createDataFrame(transDataset_y)

        # Read the metadata
        readMetaLines = sc.textFile(self.metaDataFile)

        # Filter out the first line as it is only the header (some lines could cause a problem and hence we have specific checks
        metaParts = readMetaLines.map(lambda l: l.split(","))\
                                 .filter(lambda x: (len(x) > 3) and (x[0] != 'ivrid'))

        metaDataset = metaParts.map(lambda p: Row(ivrid = p[0], songcode = p[1], businesscategory=p[2]))
        self.meta_dataFrame = sqlContext.createDataFrame(metaDataset)


    def filterSongs(self):

        self.trans_dataFrame_x = self.trans_dataFrame_x.filter(self.trans_dataFrame_x.duration_x > listenduration)
        self.trans_dataFrame_y = self.trans_dataFrame_y.filter(self.trans_dataFrame_y.duration_y > listenduration)

    def mark_liked_songs(self):

        udf = UserDefinedFunction(lambda x: likedSongs(x), IntegerType())
        likef = UserDefinedFunction(lambda x: 1 if x > 0 else 0, IntegerType())
        renamef = UserDefinedFunction(lambda x: x, StringType())

        # Add columns likes_x and likes_y to the datasets
        self.trans_dataFrame_x = self.trans_dataFrame_x.withColumn('likes_x', udf(self.trans_dataFrame_x.duration_x))
        self.trans_dataFrame_y = self.trans_dataFrame_y.withColumn('likes_y', udf(self.trans_dataFrame_y.duration_y))


        # Groupby on the song and userid and assign a value for the user and song as to whether he/she ever liked the song
        self.trans_dataFrame_x = self.trans_dataFrame_x.groupBy(self.trans_dataFrame_x.songcode_x,
                                                                self.trans_dataFrame_x.msisdn_x).max()
        self.trans_dataFrame_x = self.trans_dataFrame_x.withColumn('likes_summary_x', likef(self.trans_dataFrame_x['MAX(likes_x)']))

        # Groupby on the song and userid and assign a value for the user and song as to whether he/she ever liked the song
        self.trans_dataFrame_y = self.trans_dataFrame_y.groupBy(self.trans_dataFrame_y.songcode_y,
                                                                self.trans_dataFrame_y.msisdn_y).max()
        self.trans_dataFrame_y = self.trans_dataFrame_y.withColumn('likes_summary_y', likef(self.trans_dataFrame_y['MAX(likes_y)']))

        # Do a join with the metadata to retrieve the category for the song involved
        self.trans_dataFrame_x = self.trans_dataFrame_x.join(self.meta_dataFrame,
                                                             self.trans_dataFrame_x.songcode_x == self.meta_dataFrame.songcode,
                                                             'inner')

        # Do a join with the metadata to retrieve the category for the song involved
        self.trans_dataFrame_y = self.trans_dataFrame_y.join(self.meta_dataFrame,
                                                             self.trans_dataFrame_y.songcode_y == self.meta_dataFrame.songcode,
                                                             'inner')

        # Renaming the business category columns with suffixes x, y
        self.trans_dataFrame_x = self.trans_dataFrame_x.withColumn('businesscategory_x', renamef(self.trans_dataFrame_x['businesscategory']))
        self.trans_dataFrame_y = self.trans_dataFrame_y.withColumn('businesscategory_y', renamef(self.trans_dataFrame_y['businesscategory']))

        # Select only the components we are interested in
        self.trans_dataFrame_x = self.trans_dataFrame_x.select(self.trans_dataFrame_x['songcode_x'],
                                                               self.trans_dataFrame_x['likes_summary_x'],
                                                               self.trans_dataFrame_x['businesscategory_x'],
                                                               self.trans_dataFrame_x['msisdn_x'])

        # Select only the components we are interested in
        self.trans_dataFrame_y = self.trans_dataFrame_y.select(self.trans_dataFrame_y['songcode_y'],
                                                               self.trans_dataFrame_y['likes_summary_y'],
                                                               self.trans_dataFrame_y['businesscategory_y'],
                                                               self.trans_dataFrame_y['msisdn_y'])



    def saveSimilarityDataframe(self):

        self.similarity_dataFrame.write.save("HungamaSimilarityIntermediate.parquet",
                                             format="parquet",
                                             mode='overwrite')


    def analyzeData(self):

        self.filterSongs()

        #  Add a column indicating whether the song has been liked by the user
        self.mark_liked_songs()

        # ## Inner self join between the two datasets
        temp = self.trans_dataFrame_x.join(self.trans_dataFrame_y,
                                                               (self.trans_dataFrame_x.businesscategory_x == self.trans_dataFrame_y.businesscategory_y),
                                                                'inner')

        #  Filter out the entries where in we do not have the same msisdn as we are interested in different users
        temp = temp.filter(temp.msisdn_x != temp.msisdn_y)

        innerproduct = UserDefinedFunction(lambda x, y: x*y, IntegerType())


        ## Take the inner product between the likes columns of the dataframe such that.  1 indicates that the song was liked by both
        ## and 0 indicates that it wasn't liked by anyone
        temp = temp.withColumn('inner', innerproduct(temp.likes_summary_x, temp.likes_summary_y))

        temp.show()
        temp = temp.groupBy('businesscategory_x', 'songcode_x', 'songcode_y').agg({'likes_summary_x' : 'sum', 'inner' : 'sum', 'likes_summary_y' : 'sum'})

        projectFn = UserDefinedFunction(lambda x: x, IntegerType())

        temp = temp.withColumn('likes_x', projectFn(temp['SUM(likes_summary_x)']))
        temp = temp.withColumn('likes_y', projectFn(temp['SUM(likes_summary_y)']))
        temp = temp.withColumn('likes_xy', projectFn(temp['SUM(inner)']))

        self.similarity_dataFrame = temp.select('businesscategory_x', 'songcode_x', 'songcode_y', 'likes_x', 'likes_y', 'likes_xy')



    def show_data(self):

        print 'MetaData: '
        self.meta_dataFrame.show()
        print self.meta_dataFrame.count()


        print 'TransactionData: '
        self.trans_dataFrame_x.show()
        print self.trans_dataFrame_x.count()

        print  'Cached data:'
        self.similarity_dataFrame.show()
        self.similarity_dataFrame.count()



if __name__ == "__main__":

    sc = SparkContext(appName="HungamaOld")
    sqlContext = SQLContext(sc)

    inputFile = 'spark/data/content_data.csv'
    metaFile =  'spark/data/metadata_dataendless.csv'

    da = HungamaDataAnalyzer(inputFile=inputFile,
                             metaFile=metaFile)

    da.read_input(sc, sqlContext)
    da.analyzeData()
    da.show_data()



