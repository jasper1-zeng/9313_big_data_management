from pyspark import SparkContext, SparkConf
import sys

class Project2:   
        
    def run(self, inputPath, outputPath, stopwords, k):

        """
        Executes the Spark job.

        Args:
            input_path (str): Path to the input text file containing the dataset.
            output_path (str): Path to the output folder where results will be saved.
            stopwords (str): Number of top frequent terms to ignore (n).
            top_k (str): Number of top terms to output (k).
        """
        stopwords = int(stopwords)
        top_k = int(top_k)

        # Sets up the spark SparkContext
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        
        # Fill in your code here

        # Set up the first RDD: reads the CSV file and extract (date, term1 term2 ... ...)
        #   into (year, ( year, "term1 ... ...") )
        

        # Calculate the number of the title in each year
        #   Can use word count and using key to get the final results (year, total numbers)


        # Spilt the title into terms
        #   Calculate the TF and sum up

        
        # Stop words dealing


        # TF-IDF calcualte

        
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
