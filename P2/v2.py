from pyspark import SparkContext, SparkConf
import sys
import math

class Project2:
    """
    A Spark application that computes term weights using TF-IDF and outputs the top-k terms
    with their yearly average weights, excluding the top-n most frequent terms.
    """

    def run(self, input_path, output_path, stopwords, top_k):
        """
        Executes the Spark job.

        Args:
            input_path (str): Path to the input text file containing the dataset.
            output_path (str): Path to the output folder where results will be saved.
            stopwords (str): Number of top frequent terms to ignore (n).
            top_k (str): Number of top terms to output (k).
        """
        n = int(stopwords)
        k = int(top_k)

        # Set up the SparkContext
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)

        # Read the text file and extract (year, terms)
        lines = sc.textFile(input_path)

        def parse_line(line):
            parts = line.strip().split(',', 1)
            if len(parts) != 2:
                return None
            date, terms = parts
            year = date[:4]
            return (year, terms)

        year_terms_rdd = lines.map(parse_line).filter(lambda x: x is not None).cache()

        # Calculate the number of headlines in each year
        headlines_count_per_year = year_terms_rdd.map(lambda x: (x[0], 1)) \
            .reduceByKey(lambda a, b: a + b).collectAsMap()

        # Calculate global term counts to identify top-n frequent terms
        all_terms_rdd = year_terms_rdd.flatMap(lambda x: x[1].split())
        global_term_counts_rdd = all_terms_rdd.map(lambda term: (term, 1)) \
            .reduceByKey(lambda a, b: a + b)
        global_term_counts_list = global_term_counts_rdd.collect()
        sorted_global_term_counts = sorted(global_term_counts_list, key=lambda x: (-x[1], x[0]))
        top_n_terms_set = set([term for term, count in sorted_global_term_counts[:n]])

        # Calculate term frequencies (TF)
        term_frequency_pairs = year_terms_rdd.flatMap(
            lambda x: [((x[0], term), 1) for term in x[1].split() if term not in top_n_terms_set]
        )
        term_frequency_counts = term_frequency_pairs.reduceByKey(lambda a, b: a + b)
        term_tf_rdd = term_frequency_counts.mapValues(lambda freq: math.log10(freq))

        # Calculate inverse document frequency (IDF)
        term_document_pairs = year_terms_rdd.flatMap(
            lambda x: [((x[0], term), 1) for term in set(x[1].split()) if term not in top_n_terms_set]
        )
        term_document_counts = term_document_pairs.reduceByKey(lambda a, b: a + b)
        term_idf_rdd = term_document_counts.map(
            lambda x: (x[0], math.log10(headlines_count_per_year[x[0][0]] / x[1]))
        )

        # Calculate term weights (TF-IDF)
        tf_idf_joined_rdd = term_tf_rdd.join(term_idf_rdd)
        term_weight_rdd = tf_idf_joined_rdd.mapValues(lambda x: x[0] * x[1])

        # Compute the yearly average weight for each term
        term_weight_pairs = term_weight_rdd.map(lambda x: (x[0][1], (x[1], 1)))
        term_weight_sums = term_weight_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        term_average_weight_rdd = term_weight_sums.mapValues(lambda x: x[0] / x[1])

        # Rank the terms by their average weights in descending order, breaking ties alphabetically
        ranked_terms_rdd = term_average_weight_rdd.map(lambda x: (-x[1], x[0]))
        top_k_terms = ranked_terms_rdd.takeOrdered(k, key=lambda x: (x[0], x[1]))
        final_output_list = [(term, -weight) for weight, term in top_k_terms]

        # Output the top-k terms and their yearly average weights
        sc.parallelize(final_output_list) \
            .map(lambda x: '{}\t{:.9f}'.format(x[0], x[1])) \
            .coalesce(1).saveAsTextFile(output_path)

        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])