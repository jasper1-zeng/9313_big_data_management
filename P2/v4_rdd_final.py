from pyspark import SparkContext, SparkConf
import sys
import math

class Project2:   
        
    def run(self, input_path, output_path, stopwords, top_k):
        """
        This task is to compute the yearly average term weights in news and
        find the top-k important terms.

        Args:
            input_path: Path to the input text file.
            output_path: Path to the output folder where results will be saved.
            stopwords: Number of top frequent terms to ignore (n).
            top_k: Number of top terms to output (k).
        """

        stopwords = int(stopwords)
        top_k = int(top_k)

        # Initialize the SparkContext with the given configuration
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)
        
        # Read the input file into an RDD of lines
        lines = sc.textFile(input_path)

        # Define a function to parse each line and extract the year and terms
        def parse_line(line):
            # Split the line into date and terms based on the first comma
            date_and_terms = line.strip().split(",", 1)

            # If the line does not contain a comma or terms, skip it
            if len(date_and_terms) != 2:
                return None
            
            date, terms_in_str = date_and_terms

            if len(date) < 4:
                # If the date is not at least 4 characters long, skip it
                return None
            
            year = date[:4]  # Extract the year from the date
            terms = terms_in_str.strip().split()  # Split the terms by space
            return (year, terms)

        # Error management, filter the none value. 
        parsed_lines = lines.map(parse_line).filter(lambda x: x is not None)

        # Compute the total number of headlines per year
        # This RDD contains tuples of (year, total_headline_count)
        year_headline_counts = parsed_lines.map(lambda x: (x[0], 1)) \
                                          .reduceByKey(lambda a, b: a + b)

        # Compute the frequency of each term in each year
        # This RDD contains tuples of ((year, term), frequency)
        term_year_freq = parsed_lines.flatMap(
            lambda x: [((x[0], term), 1) for term in x[1]]
        ).reduceByKey(lambda a, b: a + b)

        # Compute the number of headlines in each year that contain each term
        # This RDD contains tuples of ((year, term), headline_count_with_term)
        term_year_headline_counts = parsed_lines.flatMap(
            lambda x: [((x[0], term), 1) for term in set(x[1])]
        ).reduceByKey(lambda a, b: a + b)

        # Compute the global count of each term across all years
        # This RDD contains tuples of (term, global_count)
        term_counts = parsed_lines.flatMap(
            lambda x: [(term, 1) for term in x[1]]
        ).reduceByKey(lambda a, b: a + b)

        # Identify the top-n frequent terms (stopwords) to be ignored
        # Map to (negative_count, term) to sort by count descending, then by term ascending
        sorted_term_counts = term_counts.map(lambda x: (-x[1], x[0])).sortByKey()
        # Collect the top-n terms based on their global counts
        stopwords_list = sorted_term_counts.take(stopwords)
        # Create a set of stopwords for efficient lookup
        stopwords_set = set([term for count_neg, term in stopwords_list])

        # Filter out stopwords from term_year_freq and term_year_headline_counts RDDs
        filtered_term_year_freq = term_year_freq.filter(lambda x: x[0][1] not in stopwords_set)
        filtered_term_year_headline_counts = term_year_headline_counts.filter(lambda x: x[0][1] not in stopwords_set)

        # Collect the total number of headlines per year into a dictionary for broadcasting
        # This will be used to compute IDF
        year_total_headlines = dict(year_headline_counts.collect())

        # Join the filtered term frequency and headline counts RDDs on (year, term)
        # The result RDD contains tuples of ((year, term), (frequency, headline_count_with_term))
        term_year_freq_and_headline_counts = filtered_term_year_freq.join(filtered_term_year_headline_counts)

        # Define a function to compute the TF-IDF weight for each term in each year
        def compute_weight(x):
            (year, term) = x[0]
            frequency = x[1][0]  # The frequency of the term in the year
            num_headlines_with_term = x[1][1]  # Number of headlines containing the term in the year
            total_headlines_in_year = year_total_headlines[year]  # Total headlines in the year
            # Compute TF using the provided formula
            TF = math.log10(frequency)
            # Compute IDF using the provided formula
            IDF = math.log10(total_headlines_in_year / num_headlines_with_term)
            # Compute the term weight as TF * IDF
            weight = TF * IDF
            # Return a tuple of (term, (weight, 1)) for aggregation
            return (term, (weight, 1))

        # Compute the TF-IDF weight for each term and map to (term, (weight, count))
        term_weights = term_year_freq_and_headline_counts.map(compute_weight)

        # Aggregate the weights and counts per term across all years
        # The result RDD contains tuples of (term, (total_weight, total_count))
        term_weights_sum_counts = term_weights.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

        # Compute the average weight per term by dividing total_weight by total_count
        # The result RDD contains tuples of (term, average_weight)
        term_average_weights = term_weights_sum_counts.map(lambda x: (x[0], x[1][0] / x[1][1]))

        # Prepare the terms for sorting by creating a composite key
        # The key is a tuple of (-average_weight, term) to sort by average_weight descending, then term ascending
        ranked_terms = term_average_weights.map(lambda x: ((-x[1], x[0]), x[1]))
        # Sort the terms based on the composite key
        sorted_terms = ranked_terms.sortByKey()

        # Collect the top-k terms with their average weights
        top_k_terms = sorted_terms.take(top_k)

        # Format the output lines as required: "term"\tWeight with 9 decimal places
        output_lines = ["\"{}\"\t{:.9f}".format(term, weight) for ((_, term), weight) in top_k_terms]

        # Create an RDD from the output lines
        output_rdd = sc.parallelize(output_lines)
        # Coalesce the RDD to a single partition and save to the specified output path
        output_rdd.coalesce(1).saveAsTextFile(output_path)

        # Stop the SparkContext to release resources
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
