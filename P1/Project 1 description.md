Here's the entire project brief converted into a **well-structured Markdown format** for readability and use in documentation or GitHub:

---

# 📊 Top-k Term Weights Computation (12 Marks)

## 🔍 Background

Detecting popular and trending topics from news articles is important for public opinion monitoring. In this project, your task is to analyze text data from a dataset of Australian news from ABC (Australian Broadcasting Corporation) using **MRJob**.

### 🎯 Goal

Compute the **top-k terms with the largest TF-IDF weights for each year** from a dataset of news article headlines.

---

## 📁 Input File Format

The input text file contains one headline per line in the format:

```
date,term1 term2 term3 ...
```

* `date` is in the format `YYYYMMDD`.
* Headline terms are space-separated.
* Common stop words (e.g., "to", "the", "in") have already been removed.

### 🔍 Example Input

```
20191124,woman stabbed adelaide shopping centre  
20191204,economy continue teetering edge recession  
20200401,coronanomics learnt coronavirus economy  
20200401,coronavirus home test kits selling chinese community  
20201015,coronavirus pacific economy foriegn aid china  
20201016,china builds pig apartment blocks guard swine flu  
20211216,economy starts bounce unemployment  
20211224,online shopping rise due coronavirus  
20211229,china close encounters elon musks  
```

Dataset available at: [Kaggle - Million Headlines](https://www.kaggle.com/therohk/million-headlines)

---

## 📐 Term Weights Computation

You must compute **TF-IDF** weights for each term in each year.

### 🧮 Formula

* **TF(term t, year y)**: frequency of `t` in `y`

* **IDF(term t, year y)**:

  ```
  log10(total headlines in y / number of headlines in y containing t)
  ```

* **Weight(term t, year y)**:

  ```
  TF(t, y) * IDF(t, y)
  ```

Use Python's `math.log10()` for logarithmic computation.

---

## ✅ Output Format

Each line should contain:

```
"Year"    "Term,Weight"
```

Sorted by:

1. Year (ascending)
2. Weight (descending)
3. Term (alphabetically)

### 🔍 Example Output (k = 2)

```
"2019"    "adelaide,0.3010299956639812"
"2019"    "centre,0.3010299956639812"
"2020"    "aid,0.6020599913279624"
"2020"    "apartment,0.6020599913279624"
"2021"    "bounce,0.47712125471966244"
"2021"    "china,0.47712125471966244"
```

---

## 🛠️ Code Instructions

The value `k` is passed using job configuration.

### 📌 Accessing `k` in MRJob

```python
from mrjob.compat import jobconf_from_env
k = int(jobconf_from_env('myjob.settings.k'))
```

### 📟 Example Hadoop Command

```bash
python3 project1.py -r hadoop input_file -o hdfs_output \
--jobconf myjob.settings.k=2 \
--jobconf mapreduce.job.reduces=2
```

> You must use **MRJob and Hadoop**. Pure Python solutions will receive **0 marks**.

---

## 📝 Submission Instructions

* **Deadline**: Wednesday 09th October, 11:59:59 PM
* **Submit via**: Ed platform
* **Late Penalty**:

  * 5% reduction for up to 5 days late
  * Over 5 days late: **submission rejected**

📧 If needed, apply for extensions via **myUNSW** and email `yi.xu10@student.unsw.edu.au`.

---

## 🧪 Marking Criteria

| Requirement                                        | Marks        |
| -------------------------------------------------- | ------------ |
| Code compiles and runs on Hadoop in Ed environment | +4           |
| All term weights are correct                       | +1           |
| Output order is correct                            | +1           |
| Efficient top-k selection                          | +1           |
| Uses combiner or in-mapper combining               | +1           |
| Implements order inversion (special keys)          | +1           |
| Implements secondary sort                          | +1           |
| Produces result with single MRStep                 | +1           |
| Works with multiple reducers                       | +1           |
| **Total**                                          | **12 marks** |

> ❌ **Disallowed**: Emitting all data directly to reducers and buffering in memory.

---

Let me know if you'd like help with the actual implementation in MRJob!
