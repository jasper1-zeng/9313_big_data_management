from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

class Project2:           
    def run(self, input_path, output_path, stopwords, top_k):
        """
        Executes the Spark job using DF.

        Args:
            input_path: Path to the input text file containing the dataset.
            output_path: Path to the output folder where results will be saved.
            stopwords: Number of top frequent terms to ignore (n).
            top_k: Number of top terms to output (k).
        """

        stopwords = int(stopwords)
        top_k = int(top_k)

        # Initialize the SparkSession
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        # Read the input file as text
        lines = spark.read.text(input_path)

        # Split each line into 'date' and 'terms'
        # The input format is 'date,terms'
        df = lines.select(
            split(col('value'), ',', 2).alias('date_and_terms')
        ).select(
            col('date_and_terms').getItem(0).alias('date'),
            col('date_and_terms').getItem(1).alias('terms')
        )

        # Extract the year from the date
        df = df.withColumn('year', substring(col('date'), 1, 4))

        # Split the 'terms' column into an array of individual terms
        df = df.withColumn('terms_array', split(col('terms'), ' '))

        # Assign a unique ID to each headline using 'monotonically_increasing_id'
        df = df.withColumn('headline_id', monotonically_increasing_id())

        # Compute the total number of headlines per year
        # This DataFrame contains ('year', 'total_headlines_in_year')
        year_headline_counts = df.groupBy('year').agg(count(lit(1)).alias('total_headlines_in_year'))

        # Explode 'terms_array' to get one term per row, along with 'year' and 'headline_id'
        # This DataFrame contains ('year', 'headline_id', 'term')
        terms_with_year_id_df = df.select(
            col('year'),
            col('headline_id'),
            explode(col('terms_array')).alias('term')
        )

        # Compute global term counts to identify stopwords
        # Group by 'term' and count the number of occurrences across all headlines
        # This DataFrame contains ('term', 'global_count')
        term_counts = terms_with_year_id_df.groupBy('term').agg(count(lit(1)).alias('global_count'))

        # Identify the top-n frequent terms (stopwords) to be ignored
        # Order terms by 'global_count' descending and 'term' ascending to break ties alphabetically
        from pyspark.sql.window import Window

        windowSpec = Window.orderBy(col('global_count').desc(), col('term').asc())
        # Add a 'rank' column to rank the terms
        term_counts = term_counts.withColumn('rank', row_number().over(windowSpec))
        # Collect the top-n stopwords into a list
        stopwords_list = term_counts.filter(col('rank') <= stopwords).select('term').collect()
        # Create a set of stopwords for efficient lookup
        stopwords_set = set(row['term'] for row in stopwords_list)

        # Create a DataFrame for stopwords to use in joins
        stopwords_df = spark.createDataFrame([(term,) for term in stopwords_set], ['term'])

        # Filter out stopwords by performing a left anti join between 'terms_with_year_id_df' and 'stopwords_df'
        # This retains rows in 'terms_with_year_id_df' where 'term' is not in 'stopwords_df'
        filtered_terms_df = terms_with_year_id_df.join(stopwords_df, on='term', how='left_anti')

        # Compute the frequency of each term in each year
        # Group by 'year' and 'term' and count the total occurrences
        # This DataFrame contains ('year', 'term', 'frequency')
        term_year_freq = filtered_terms_df.groupBy('year', 'term').agg(count(lit(1)).alias('frequency'))

        # Compute the number of headlines in each year that contain each term
        # Group by 'year' and 'term' and count distinct 'headline_id's
        # This DataFrame contains ('year', 'term', 'num_headlines_with_term')
        term_year_headline_counts = filtered_terms_df.groupBy('year', 'term') \
                                                    .agg(countDistinct('headline_id').alias('num_headlines_with_term'))

        # Join 'term_year_freq' and 'term_year_headline_counts' on ('year', 'term')
        term_stats_df = term_year_freq.join(term_year_headline_counts, on=['year', 'term'])

        # Join with 'year_headline_counts' to get 'total_headlines_in_year' for IDF calculation
        term_stats_df = term_stats_df.join(year_headline_counts, on='year')

        # Compute TF (Term Frequency) using the formula: TF = log10(frequency)
        term_stats_df = term_stats_df.withColumn('TF', log10(col('frequency')))
        # Compute IDF (Inverse Document Frequency) using the formula: IDF = log10(total_headlines_in_year / num_headlines_with_term)
        term_stats_df = term_stats_df.withColumn('IDF', log10(col('total_headlines_in_year') / col('num_headlines_with_term')))
        # Compute the term weight as Weight = TF * IDF
        term_stats_df = term_stats_df.withColumn('Weight', col('TF') * col('IDF'))

        # Compute the average weight per term over all years
        # Group by 'term' and compute the average of 'Weight'
        # This DataFrame contains ('term', 'avg_weight')
        term_avg_weight_df = term_stats_df.groupBy('term').agg(avg('Weight').alias('avg_weight'))

        # Rank the terms by average weight descending, then term ascending
        ranked_terms_df = term_avg_weight_df.orderBy(col('avg_weight').desc(), col('term').asc())

        # Get the top-k terms
        top_k_terms_df = ranked_terms_df.limit(top_k)

        # Format the output as required: "term"\tWeight with 9 decimal places
        # Use 'format_string' to format each line
        output_df = top_k_terms_df.select(
            format_string('"%s"\t%.9f', col('term'), col('avg_weight')).alias('output_line')
        )

        # Save the output to the specified output path
        # Use 'coalesce(1)' to write the output into a single file
        output_df.coalesce(1).write.mode('overwrite').text(output_path)
        
        # Stop the SparkSession to release resources
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

