# **Average Term Weights Computation (16 marks)**

## **Background**

Detecting popular and trending topics from news articles is important for public opinion monitoring. In this project, your task is to analyze text data over a dataset of Australian news from ABC (Australian Broadcasting Corporation) using **Apache Spark**.

The problem is to compute the **yearly average term weights** in the news articles dataset and find the **top-k important terms**.

---

## **Input Format**

Each line in the dataset represents a headline, formatted as:

```
date,term1 term2 term3 ...
```

* **Date**: in the format `yyyymmdd`
* **Terms**: space-separated list of words from the headline

### **Example Input**

```
20030219,council chief executive fails to secure position  
20030219,council welcomes ambulance levy decision  
20030219,council welcomes insurance breakthrough  
20030219,fed opp to re introduce national insurance  
20040501,cowboys survive eels comeback  
20040501,cowboys withstand eels fightback  
20040502,castro vows cuban socialism to survive bush  
20200401,coronanomics things learnt about how coronavirus economy  
20200401,coronavirus at home test kits selling in the chinese community  
20200401,coronavirus campbell remess streams bear making classes  
20201016,china builds pig apartment blocks to guard against swine flu  
```

---

## **Term Weight Computation**

To compute the **weight for a term** in a specific year, use the **TF-IDF** model:

### **1. Term Frequency (TF)**

```python
TF(term t, year y) = log10(frequency of term t in year y)
```

### **2. Inverse Document Frequency (IDF)**

```python
IDF(term t, year y) = log10(number of headlines in year y / number of headlines in year y that contain term t)
```

### **3. Weight**

```python
Weight(term t, year y) = TF(t, y) * IDF(t, y)
```

> Use `math.log10()` for logarithmic calculations. Precision differences can be ignored.

---

## **Filtering Common Terms**

Highly frequent (common/stop) terms should be filtered out:

* Compute **global count** of each term
* Rank terms by:

  * Count (descending)
  * Alphabetical order (for ties)
* Remove the **top-n** terms

---

## **Output Requirements**

* Compute **average term weights per year**
* Rank terms by:

  1. Average weight (descending)
  2. Term (alphabetically for ties)
* Output **top-k terms** in the format:

```
term\tWeight
```

### **Example Output (n=1, k=5):**

```
insurance    0.090619058
welcomes     0.090619058
coronavirus  0.059610927
council      0.059610927
cowboys      0.053008751
```

---

## **Code Submission**

You must submit **two solutions**:

* One using **only RDD APIs**
* One using **only DataFrame APIs**

Each script should accept **four parameters**:

```bash
spark-submit project2_rdd.py "file:///home/abcnews.txt" "file:///home/output" 1 5
```

Where:

* Input file path (with `file:///` prefix)
* Output folder path
* `n` = number of most frequent terms to ignore
* `k` = number of top terms to output

---

## **Constraints**

* **Do not use** `numpy` or `pandas`
* Use `coalesce(1)` to merge into one output file
* For DataFrame solution: **do not use `spark.sql()`**
* Output file can have or not have a final newline
* You may read files from HDFS or local (must prefix with `file:///` for local)

---

## **Deadline**

**Thursday 24th October, 11:59:59 PM**

If an extension is needed, apply via **myUNSW**.

---

## **Submission Notes**

* Submit multiple times; only the last one will be graded
* Keep a screenshot as proof of successful submission
* Email any issues to: `yi.xu10@student.unsw.edu.au`

---

## **Late Submission Penalty**

* **5% per day** (up to 5 days)
* Submissions more than 5 days late will be **rejected**

---

## **Marking Criteria**

| Criteria                                                      | Marks |
| ------------------------------------------------------------- | ----- |
| Spark job compiles and runs successfully                      | +3    |
| Correct top-k output (terms, weights, order)                  | +3    |
| Proper use of Spark APIs (RDD or DataFrame only, as required) | +0.5  |
| Code readability, structure, and documentation                | +0.5  |
| Efficient removal of high-frequency terms                     | +1    |
| **Total per solution**                                        | **8** |
