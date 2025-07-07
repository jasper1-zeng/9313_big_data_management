import sys
import math
from pyspark import SparkConf, SparkContext

class Project3:
    def run(self, inputpath, outputpath, d_threshold, s_threshold):
        """
        The main method to execute the similarity join project.

        Parameters:
        - inputpath (str): Path to the input file containing records.
        - outputpath (str): Path to the output directory where results will be saved.
        - d_threshold (str): Distance threshold 'd' as a string, to be converted to float.
        - s_threshold (str): Similarity threshold 's' as a string, to be converted to float.
        """

        # Initialize Spark configuration and context
        conf = SparkConf().setAppName("SimilarityJoin")
        sc = SparkContext(conf=conf)

        # Convert threshold arguments to float
        d_threshold = float(d_threshold)
        s_threshold = float(s_threshold)

        # Broadcast the thresholds to all worker nodes for efficient access
        d_threshold_broadcast = sc.broadcast(d_threshold)
        s_threshold_broadcast = sc.broadcast(s_threshold)

        # Read the input data as an RDD of lines
        data = sc.textFile(inputpath)

        # Parse each line into a structured record and cache the RDD for performance
        records = data.map(self.parse_record).cache()

        # Generate prefixes and spatial grid cells for each record
        record_prefixes = records.flatMap(
            lambda record: self.generate_prefixes_and_cells(
                record,
                s_threshold_broadcast.value,
                d_threshold_broadcast.value
            )
        )

        # Group records by their grid cell and prefix term to identify candidate pairs
        candidates = record_prefixes.groupByKey().flatMap(
            lambda x: self.generate_candidate_pairs(x[1])
        )

        # Compute similarity and distance for each candidate pair and filter based on thresholds
        results = candidates.map(
            lambda pair: self.compute_similarity_and_distance(
                pair,
                s_threshold_broadcast.value,
                d_threshold_broadcast.value
            )
        ).filter(lambda x: x is not None)  # Retain only valid pairs

        # Remove duplicate pairs and sort the results by record IDs in ascending order
        final_results = results.distinct().sortBy(lambda x: (x[0][0], x[0][1]))

        # Format the final results as specified and save to the output directory
        output = final_results.map(
            lambda x: self.format_output(x)
        )
        output.saveAsTextFile(outputpath)

        # Stop the Spark context to release resources
        sc.stop()

    def parse_record(self, line):
        """
        Parses a single line of input data into a structured record.

        Input Line Format:
        "id#(x,y)#term1 term2 term3 ..."

        Parameters:
        - line (str): A line from the input file.

        Returns:
        - tuple: (record_id (int), (x (float), y (float)), sorted_terms (list of str))
        """

        # Split the line by '#' to extract components
        parts = line.strip().split('#')

        # Extract and convert the record ID to integer
        record_id = int(parts[0])

        # Extract and convert the spatial coordinates to floats
        x, y = map(float, parts[1].strip('()').split(','))

        # Split the textual description into individual terms and sort them
        terms = parts[2].split()
        terms.sort()

        return (record_id, (x, y), terms)

    def compute_prefix_length(self, terms_len, s):
        """
        Computes the prefix length for a record based on the Jaccard similarity threshold.

        The prefix length determines how many terms are used for indexing to reduce candidate pairs.

        Parameters:
        - terms_len (int): Total number of terms in the record's textual description.
        - s (float): Similarity threshold.

        Returns:
        - int: Calculated prefix length.
        """
        return terms_len - int(math.floor(s * terms_len)) + 1

    def generate_prefixes_and_cells(self, record, s_threshold, d_threshold):
        """
        Generates key-value pairs for each record based on its prefixes and spatial grid cells.

        For each prefix term, the record is assigned to its own grid cell and all neighboring cells
        to ensure that all potential close pairs are considered.

        Parameters:
        - record (tuple): A structured record as returned by parse_record.
        - s_threshold (float): Similarity threshold.
        - d_threshold (float): Distance threshold.

        Returns:
        - list of tuples: Each tuple is ((cell_x, cell_y, prefix_term), record_info)
        """

        record_id, (x, y), terms = record
        terms_len = len(terms)

        # Handle records with no terms by returning an empty list
        if terms_len == 0:
            return []

        # Compute the prefix length based on the similarity threshold
        prefix_length = self.compute_prefix_length(terms_len, s_threshold)
        prefix_length = min(prefix_length, terms_len)  # Ensure it does not exceed the number of terms

        # Extract the prefix terms
        prefixes = terms[:prefix_length]

        # Determine the size of each grid cell based on the distance threshold
        cell_size = d_threshold

        # Compute the grid cell coordinates for the record's location
        grid_x = int(math.floor(x / cell_size))
        grid_y = int(math.floor(y / cell_size))

        # Generate a list of neighboring cells (including the cell itself)
        cells = []
        for i in range(grid_x - 1, grid_x + 2):
            for j in range(grid_y - 1, grid_y + 2):
                cells.append((i, j))

        # Generate key-value pairs for each combination of cell and prefix term
        results = []
        for cell in cells:
            for prefix in prefixes:
                key = (cell, prefix)  # Key consists of grid cell and prefix term
                value = (record_id, x, y, terms)  # Value contains record information
                results.append((key, value))

        return results

    def generate_candidate_pairs(self, records):
        """
        Generates all unique candidate pairs from a group of records sharing the same prefix and grid cell.

        Ensures that each pair is considered only once by maintaining the condition record1.id < record2.id.

        Parameters:
        - records (iterable): An iterable of record information tuples.

        Returns:
        - list of tuples: Each tuple is ((record1, record2))
        """
        
        # Convert the iterable to a list for processing
        records = list(records)

        # Sort the records by their IDs to maintain order and uniqueness
        records.sort(key=lambda x: x[0])  # Sort by record_id

        candidates = []
        num_records = len(records)

        # Generate all possible unique pairs without duplication
        for i in range(num_records):
            for j in range(i + 1, num_records):
                record1 = records[i]
                record2 = records[j]

                # Ensure that record1.id is less than record2.id to avoid duplicates
                if record1[0] < record2[0]:
                    candidates.append((record1, record2))

        return candidates

    def compute_similarity_and_distance(self, pair, s_threshold, d_threshold):
        """
        Computes the Euclidean distance and Jaccard similarity for a pair of records.

        Filters out pairs that do not meet the specified thresholds.

        Parameters:
        - pair (tuple): A tuple containing two records.
        - s_threshold (float): Similarity threshold.
        - d_threshold (float): Distance threshold.

        Returns:
        - tuple or None: ((id1, id2), distance, jaccard_similarity) if thresholds are met; otherwise, None.
        """
        record1, record2 = pair
        id1, x1, y1, terms1 = record1
        id2, x2, y2, terms2 = record2

        # Compute Euclidean distance between the two spatial locations
        distance = math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)
        if distance > d_threshold:
            return None  # Discard pairs exceeding the distance threshold

        # Compute Jaccard similarity between the two term sets
        set1 = set(terms1)
        set2 = set(terms2)
        intersection = set1.intersection(set2)
        union = set1.union(set2)

        if not union:
            return None  # Avoid division by zero if both term sets are empty

        jaccard_similarity = float(len(intersection)) / len(union)

        if jaccard_similarity < s_threshold:
            return None  # Discard pairs below the similarity threshold

        # Round the distance and similarity to six decimal places
        distance = round(distance, 6)
        jaccard_similarity = round(jaccard_similarity, 6)

        # Return the valid pair with computed metrics
        return ((id1, id2), distance, jaccard_similarity)

    def format_output(self, record):
        """
        Formats the final output string for a valid pair, ensuring proper decimal formatting.

        Example Formats:
        - (0,2):1.414214, 0.75
        - (2,3):1.0, 0.5

        Parameters:
        - record (tuple): A tuple containing ((id1, id2), distance, jaccard_similarity).

        Returns:
        - str: Formatted string as per the specified output format.
        """
        pair, distance, similarity = record

        # Format distance and similarity by removing unnecessary trailing zeros
        formatted_distance = self.trim_trailing_zeros(distance)
        formatted_similarity = self.trim_trailing_zeros(similarity)

        # Construct the output string
        return f"({pair[0]},{pair[1]}):{formatted_distance}, {formatted_similarity}"

    def trim_trailing_zeros(self, num):
        """
        Converts a float to a string, removing trailing zeros and ensuring at least one decimal place.

        Specific Formatting Rules:
        - 0.250000 -> "0.25"
        - 1.000000 -> "1.0"

        Parameters:
        - num (float): The number to format.

        Returns:
        - str: Formatted string without unnecessary trailing zeros.
        """
        # Format the number to six decimal places
        formatted = '{0:.6f}'.format(num).rstrip('0').rstrip('.')

        # Check if the original formatted string contained a decimal point
        if '.' in '{0:.6f}'.format(num):
            # If the trimmed string lacks a decimal point, append '.0' to ensure one decimal place
            if '.' not in formatted:
                formatted += '.0'
        else:
            # If no decimal point exists, append '.0' to maintain consistency
            formatted += '.0'

        return formatted

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    
