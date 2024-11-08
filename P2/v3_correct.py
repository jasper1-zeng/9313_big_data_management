from pyspark import SparkContext, SparkConf
import sys
import math

class Project2:   
        
    def run(self, input_path, output_path, stopwords, top_k):
        """
        This task is to compute the yearly average term weights in news and find the top-k important terms

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
        
        # Read the input file into an RDD
        lines = sc.textFile(input_path)

        # Parse lines into (year, [terms])
        def parse_line(line):
            # Split the line into date and terms based on the first comma
            date_and_terms = line.strip().split(",", 1)

            if len(date_and_terms) != 2:
                return None
            date, terms_str = date_and_terms
            if len(date) < 4:
                return None
            
            
            year = date[:4]
            terms = terms_str.strip().split()
            return (year, terms)

        parsed_lines = lines.map(parse_line).filter(lambda x: x is not None)

        # Compute total number of headlines per year
        year_headline_counts = parsed_lines.map(lambda x: (x[0], 1)).reduceByKey(lambda a,b: a+b)

        # Compute term frequencies per (year, term)
        term_year_freq = parsed_lines.flatMap(
            lambda x: [((x[0], term), 1) for term in x[1]]
        ).reduceByKey(lambda a,b: a+b)

        # Compute number of headlines in year y containing term t
        term_year_headline_counts = parsed_lines.flatMap(
            lambda x: [((x[0], term), 1) for term in set(x[1])]
        ).reduceByKey(lambda a,b: a+b)

        # Compute global term counts for stopwords
        term_counts = parsed_lines.flatMap(
            lambda x: [(term, 1) for term in x[1]]
        ).reduceByKey(lambda a,b: a+b)

        # Get stopwords: top-n terms by global counts, breaking ties alphabetically
        # Map to (count, term) and sort
        sorted_term_counts = term_counts.map(lambda x: (-x[1], x[0])).sortByKey()
        stopwords_list = sorted_term_counts.take(stopwords)
        stopwords_set = set([term for count_neg, term in stopwords_list])

        # Filter out stopwords from term_year_freq and term_year_headline_counts
        filtered_term_year_freq = term_year_freq.filter(lambda x: x[0][1] not in stopwords_set)
        filtered_term_year_headline_counts = term_year_headline_counts.filter(lambda x: x[0][1] not in stopwords_set)

        # Collect year_headline_counts to driver
        year_total_headlines = dict(year_headline_counts.collect())

        # Join filtered_term_year_freq and filtered_term_year_headline_counts
        term_year_freq_and_headline_counts = filtered_term_year_freq.join(filtered_term_year_headline_counts)

        # Compute TF, IDF, and weight
        def compute_weight(x):
            (year, term) = x[0]
            frequency = x[1][0]  # frequency of term in year
            num_headlines_with_term = x[1][1]  # number of headlines in year having term
            total_headlines_in_year = year_total_headlines[year]
            TF = math.log10(frequency)
            IDF = math.log10(total_headlines_in_year / num_headlines_with_term)
            weight = TF * IDF
            return (term, (weight, 1))

        term_weights = term_year_freq_and_headline_counts.map(compute_weight)

        # Sum weights and counts per term
        term_weights_sum_counts = term_weights.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))

        # Compute average weight per term
        term_average_weights = term_weights_sum_counts.map(lambda x: (x[0], x[1][0]/x[1][1]))

        # Rank terms by average weight descending, then by term alphabetically
        ranked_terms = term_average_weights.map(lambda x: ((-x[1], x[0]), x[1]))
        sorted_terms = ranked_terms.sortByKey()

        # Get top-k terms
        top_k_terms = sorted_terms.take(top_k)

        # Format output
        output_lines = ["\"{}\"\t{:.9f}".format(term, weight) for ((_, term), weight) in top_k_terms]

        # Save output
        output_rdd = sc.parallelize(output_lines)
        output_rdd.coalesce(1).saveAsTextFile(output_path)

        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
