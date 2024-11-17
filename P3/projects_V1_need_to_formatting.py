import sys
import math
from pyspark import SparkConf, SparkContext

class Project3:
    def run(self, inputpath, outputpath, d_threshold, s_threshold):
        # Initialize SparkConf and SparkContext
        conf = SparkConf().setAppName("SimilarityJoin")
        sc = SparkContext(conf=conf)

        # Broadcast the distance and similarity thresholds
        d_threshold = float(d_threshold)
        s_threshold = float(s_threshold)
        d_threshold_broadcast = sc.broadcast(d_threshold)
        s_threshold_broadcast = sc.broadcast(s_threshold)

        # Read and parse the input data
        data = sc.textFile(inputpath)
        records = data.map(self.parse_record)

        # Generate prefixes and grid cells for each record
        record_prefixes = records.flatMap(
            lambda record: self.generate_prefixes_and_cells(record, s_threshold_broadcast.value, d_threshold_broadcast.value)
        )

        # Group records by (grid cell id, prefix term)
        candidates = record_prefixes.groupByKey().flatMap(
            lambda x: self.generate_candidate_pairs(x[1])
        )

        # Compute similarities and distances, and filter pairs
        results = candidates.map(
            lambda pair: self.compute_similarity_and_distance(pair, s_threshold_broadcast.value, d_threshold_broadcast.value)
        ).filter(lambda x: x is not None)

        # Remove duplicates and sort the results
        final_results = results.distinct().sortBy(lambda x: (x[0][0], x[0][1]))

        # Format and save the output
        output = final_results.map(
            lambda x: f"({x[0][0]},{x[0][1]}):{x[1]:.6f}, {x[2]:.6f}"
        )
        
        output.saveAsTextFile(outputpath)
        sc.stop()

    def parse_record(self, line):
        # Parse each line into (id, (x, y), sorted terms)
        parts = line.strip().split('#')
        record_id = int(parts[0])
        x, y = map(float, parts[1].strip('()').split(','))
        terms = parts[2].split()
        terms.sort()
        return (record_id, (x, y), terms)

    def compute_prefix_length(self, terms_len, s):
        # Compute the prefix length using the formula
        return terms_len - int(math.floor(s * terms_len)) + 1

    def generate_prefixes_and_cells(self, record, s_threshold, d_threshold):
        record_id, (x, y), terms = record
        terms_len = len(terms)
        prefix_length = self.compute_prefix_length(terms_len, s_threshold)
        prefixes = terms[:prefix_length]

        # Determine grid cells (own cell and neighboring cells)
        cell_size = d_threshold
        grid_x = int(x / cell_size)
        grid_y = int(y / cell_size)
        cells = []
        for i in range(grid_x - 1, grid_x + 2):
            for j in range(grid_y - 1, grid_y + 2):
                cells.append((i, j))

        # Generate key-value pairs
        results = []
        for cell in cells:
            for prefix in prefixes:
                key = (cell, prefix)
                value = (record_id, x, y, terms)
                results.append((key, value))
        return results

    def generate_candidate_pairs(self, records):
        # Generate candidate pairs where record1.id < record2.id
        records = list(records)
        records.sort(key=lambda x: x[0])  # Sort by record id
        candidates = []
        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                record1 = records[i]
                record2 = records[j]
                if record1[0] < record2[0]:
                    candidates.append((record1, record2))
        return candidates

    def compute_similarity_and_distance(self, pair, s_threshold, d_threshold):
        record1, record2 = pair
        id1, x1, y1, terms1 = record1
        id2, x2, y2, terms2 = record2

        # Compute Euclidean distance
        distance = math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)
        if distance > d_threshold:
            return None

        # Compute Jaccard similarity
        set1 = set(terms1)
        set2 = set(terms2)
        intersection = set1.intersection(set2)
        union = set1.union(set2)
        jaccard_similarity = float(len(intersection)) / len(union)
        if jaccard_similarity < s_threshold:
            return None

        # Round the distance and similarity
        distance = round(distance, 6)
        jaccard_similarity = round(jaccard_similarity, 6)

        # Return the result
        return ((id1, id2), distance, jaccard_similarity)

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    
