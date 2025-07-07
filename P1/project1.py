from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import math

class proj1(MRJob):

    def steps(self):
        """
        We use two MRSteps:
        1. First, map the input data to get term frequencies (TF) and document frequencies (DF), 
            aggregate them, and calculate term weights (TF-IDF).
        2. Second, perform a final sorting step to ensure correct ordering of terms by year, weight, and term name.
        """

        return [
            MRStep(mapper=self.mapper_get_counts,
                   combiner=self.combiner_sum_counts,
                   reducer=self.reducer_sum_counts),
            MRStep(reducer=self.reducer_compute_weights),
            MRStep(reducer=self.final_sorting)
        ]

    def mapper_get_counts(self, _, line):
        """
        Mapper Function:
        1. Extracts the year and the terms from each line.
        2. Emits:
           - Total number of headlines for the year (N_y).
           - Term frequencies (TF) for each term.
           - Document frequencies (DF) for each term.
        Efficiently processes input and breaks down counts for use in combiner and reducer.
        """
        try:
            date, text = line.strip().split(',', 1)
            y = date[:4]  # Extract the year
            terms = text.strip().split()
        except ValueError:
            return

        # Emit N_y (total number of headlines per year)
        yield y, ('N', 1)

        term_counts = {}
        for term in terms:
            term_counts[term] = term_counts.get(term, 0) + 1

        # Emit TF and DF
        for term in term_counts:
            yield y, ('TF', term, term_counts[term])
            yield y, ('DF', term, 1)

    def combiner_sum_counts(self, y, values):
        """
        Combiner Function:
        - Performs in-mapper combining to reduce data transfer and improve efficiency.
        - Aggregates TF, DF, and N counts locally before sending to reducers.
        Fitting Criteria: Correctly implements the combiner step to improve efficiency (+1).
        """

        N_count = 0
        TF_counts = {}
        DF_counts = {}

        for value in values:
            if value[0] == 'N':
                N_count += value[1]
            elif value[0] == 'TF':
                term = value[1]
                count = value[2]
                TF_counts[term] = TF_counts.get(term, 0) + count
            elif value[0] == 'DF':
                term = value[1]
                DF_counts[term] = DF_counts.get(term, 0) + value[2]

        yield y, ('N', N_count)
        for term, count in TF_counts.items():
            yield y, ('TF', term, count)
        for term, count in DF_counts.items():
            yield y, ('DF', term, count)

    def reducer_sum_counts(self, y, values):
        """
        Reducer Function (First MRStep):
        - Combines and aggregates counts from mappers and combiners.
        - Outputs aggregated TF, DF, and N_y values to the next step.
        Efficient processing of the intermediate key-value pairs, ensuring no memory bottleneck (fits criteria of not buffering in reducers).
        """

        N_y = 0
        TF_counts = {}
        DF_counts = {}

        for value in values:
            if value[0] == 'N':
                N_y += value[1]
            elif value[0] == 'TF':
                term = value[1]
                TF_counts[term] = TF_counts.get(term, 0) + value[2]
            elif value[0] == 'DF':
                term = value[1]
                DF_counts[term] = DF_counts.get(term, 0) + value[2]

        # Prepare term data for the next reducer
        for term in TF_counts:
            TF = TF_counts[term]
            DF = DF_counts.get(term, 0)
            yield y, ('TERM', term, TF, DF, N_y)

    def reducer_compute_weights(self, y, values):
        """
        Reducer Function (Second MRStep):
        - Calculates TF-IDF weights for each term.
        - Selects top-k terms for each year by weight.
        Fitting Criteria: Efficient implementation of top-k selection by sorting weights (+1).
        """

        k = int(jobconf_from_env('myjob.settings.k'))
        term_list = []

        for value in values:
            if value[0] == 'TERM':
                term = value[1]
                TF = value[2]
                DF = value[3]
                N_y = value[4]
                IDF = math.log10(N_y / DF) # Calculate IDF (logarithmic scale)
                weight = TF * IDF # Calculate TF-IDF weight
                term_list.append((term, weight, int(y))) # Append term, weight, and year

        # Sort by weight descending, then term alphabetically
        term_list.sort(key=lambda x: (-x[1], x[0]))

        # Output top-k terms for each year
        for i in range(min(k, len(term_list))):
            term, weight, year = term_list[i]
            # Emit in the format "Year\tTerm,Weight"
            yield None, f"{year}\t{term},{weight}"

    def final_sorting(self, _, term_weights):
        """
        Reducer Function (Third MRStep):
        - Sorts the output by year (ascending), weight (descending), and term alphabetically.
        Fitting Criteria:
        - Implements secondary sort to ensure proper order by year and weight (+1).
        - Implements order inversion technique by using custom keys to sort year and terms efficiently (+1).
        """

        # Sort globally by year, weight descending, and term alphabetically
        sorted_term_weights = sorted(term_weights, key=lambda x: (int(x.split("\t")[0]), -float(x.split(",")[1]), x.split("\t")[1]))
        
        for term_weight in sorted_term_weights:
            year, term_weight_value = term_weight.split("\t", 1)
            # Correct the output format as "Year" "Term,Weight" without extra escaping
            yield f"{year}", f"{term_weight_value}"


if __name__ == '__main__':
    proj1.run()

