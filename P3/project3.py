import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class project3:
    def run(self, inputpath, outputpath, d, s):
        # List the dealing thoughts that fit the requirements

        # Initialize the SparkContext with the given configuration

        




        
if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    

