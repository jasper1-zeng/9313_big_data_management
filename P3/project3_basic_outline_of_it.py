import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *






class project3:
    def run(self, inputpath, outputpath, d, s):
        # List the dealing thoughts that fit the requirements

        # 1. Initialize the SparkConf(), SparkContext

        # 2. Extract ID, locations and terms (seperated by #)
        # input format:
        """
        0#(37.141909,98.427776)#aba decides community broadcasting licence
        1#(52.821524,-91.929612)#kelly disgusted alleged bp ethanol scare
        2#(-1.274188,-24.540353)#palestinians killed gaza incursion
        ...
        """

        # 3. Can use prefix filtering
            # rank the words in term
            # calculate the length of prefix using term size and threshold s
        


        # 4. Calculate the distance and similarity in the same group if they have the same prefix
            # First to group, and make sure more than 2 elements
            # Literate to calculate distance and similarity (make sure each pair just calculate only once)

        
        # 5. remove duplicate
            # Can use reducebykey
        
        # 6. Rank, output and save the results


        
if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    

