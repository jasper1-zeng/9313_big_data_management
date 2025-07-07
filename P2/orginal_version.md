# **Assignment: Average Term Weights in News Headlines (16 marks)**

## **Overview**

In this assignment, you will analyze a dataset of Australian news headlines from the ABC (Australian Broadcasting Corporation) to detect important and trending topics over time. Your goal is to compute the **average term weights per year** using **Apache Spark**, and identify the **top-k most significant terms** across the dataset.

You are required to implement two separate versions:

* One using **RDD APIs**
* One using **DataFrame APIs**

---

## **Dataset Format**

The input file contains one news headline per line. Each line follows the format:

```
YYYYMMDD,term1 term2 term3 ...
```

* The date and text are separated by a comma.
* The text is a space-separated list of words (terms).

### **Example Input**

```
20030219,council chief executive fails to secure position  
20040501,cowboys survive eels comeback  
20200401,coronavirus test kits selling in the chinese community
```

---

## **Task Details**

### **Term Weight Calculation (TF-IDF)**

You will compute term weights using the **TF-IDF** model, applied per **year**.

1. **TF (Term Frequency):**

   $$
   \text{TF}(t, y) = \log_{10}(\text{total count of term } t \text{ in year } y)
   $$

2. **IDF (Inverse Document Frequency):**

   $$
   \text{IDF}(t, y) = \log_{10}\left(\frac{\text{total number of headlines in year } y}{\text{number of headlines in year } y \text{ that contain } t}\right)
   $$

3. **Weight:**

   $$
   \text{Weight}(t, y) = \text{TF}(t, y) \times \text{IDF}(t, y)
   $$

You must use `math.log10()` for all logarithmic operations.

### **Average Weight Across Years**

For each term, calculate the **average of its weights across all years** in which it appears.

---

## **Filtering Common Terms**

Some highly frequent words (e.g., "the", "to", etc.) need to be excluded from the analysis.

To do this:

* Count the **global frequency** of each term across the entire dataset.
* Rank terms by:

  1. Frequency (descending)
  2. Alphabetical order (as a tie-breaker)
* Remove the **top-n most common terms** from further processing.

---

## **Output Specification**

You must output the **top-k terms** with the highest average weights, sorted by:

1. **Average weight (descending)**
2. **Term (alphabetical order as a tie-breaker)**

Each output line should be in this format:

```
term<TAB>weight
```

> Note: You may ignore small differences in floating-point precision.

### **Example Output (n = 1, k = 5)**

```
insurance	0.090619058  
welcomes	0.090619058  
coronavirus	0.059610927  
council	0.059610927  
cowboys	0.053008751
```

---

## **How to Run**

Each script must accept the following arguments:

```
spark-submit project2_rdd.py "file:///home/abcnews.txt" "file:///home/output" 1 5
```

Where:

* First argument: path to the input file (with `file:///` prefix for local files)
* Second argument: output folder path (also prefixed)
* Third argument: `n` (number of common terms to ignore)
* Fourth argument: `k` (number of top terms to return)

---

## **Requirements**

* **Do not use** `numpy` or `pandas`
* For RDD version: use **only RDD APIs**
* For DataFrame version: use **only DataFrame APIs** (no `spark.sql`)
* Use `coalesce(1)` to write the output as a single file
* You may read input from either HDFS or local files (use `file:///` prefix for local)

---

## **Submission Instructions**

* **Deadline:** Thursday, 24th October @ 11:59:59 PM
* Submit via the submission portal. Multiple submissions allowed; only the last one is marked.
* Keep a screenshot of your successful submission as proof.
* For issues, email: `yi.xu10@student.unsw.edu.au`

---

## **Late Submission Policy**

* **-5% per day** (up to 5 days)
* Submissions **more than 5 days late will not be accepted**

---

## **Grading Criteria (per solution)**

| Criteria                                                | Marks |
| ------------------------------------------------------- | ----- |
| Code runs and compiles on Spark                         | +3    |
| Produces correct top-k results                          | +3    |
| Correct and exclusive use of RDD/DataFrame APIs         | +0.5  |
| Code is well-structured and readable with documentation | +0.5  |
| Efficient implementation of frequent term filtering     | +1    |
| **Total per version**                                   | **8** |

> You must submit both the RDD and DataFrame versions (each worth 8 marks).
