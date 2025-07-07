# **Project: Spatial and Textual Similarity Join (22 Marks)**

## **Overview**

In this project, you'll perform a **similarity join** based on both **spatial proximity** and **textual similarity** using **Apache Spark**.

Each record $R = (L, S)$ contains:

* `L`: a spatial location represented by a 2D coordinate (x, y)
* `S`: a set of textual terms (i.e., keywords)

---

## **Objective**

Find **all pairs** of records that are:

1. **Close in space** (based on Euclidean distance)
2. **Textually similar** (based on Jaccard similarity)

Each valid pair must satisfy:

* **Euclidean distance ≤ `d`**

  $$
  \sqrt{(R_1.L.x - R_2.L.x)^2 + (R_1.L.y - R_2.L.y)^2} \leq d
  $$
* **Jaccard similarity ≥ `s`**

  $$
  \text{Jaccard}(R_1.S, R_2.S) = \frac{|R_1.S \cap R_2.S|}{|R_1.S \cup R_2.S|} \geq s
  $$

---

## **Example**

Given the input records:

| id | location | terms   |
| -- | -------- | ------- |
| 0  | (0, 0)   | a d e f |
| 1  | (4, 3)   | b c f   |
| 2  | (1, 1)   | d e f   |
| 3  | (1, 2)   | a d f   |
| 4  | (2, 1)   | b e f   |
| 5  | (5, 5)   | c e     |

With thresholds:

* Distance $d = 2$
* Similarity $s = 0.5$

### **Expected Output**

```
(0,2):1.414214, 0.75
(2,3):1.0, 0.5
(2,4):1.0, 0.5
```

---

## **Output Format**

Each line of the output should follow the format:

```
(record1.id,record2.id):distance, similarity
```

**Requirements:**

* `record1.id < record2.id`
* **Round** distance and similarity values to **6 decimal places**
* No duplicate pairs
* Output sorted by `record1.id`, then `record2.id` (ascending)

---

## **Running Your Code**

Use the following command to execute your Spark job:

```bash
$ spark-submit project3.py input output d s
```

Where:

* `input`: path to the input file
* `output`: output folder path
* `d`: distance threshold
* `s`: similarity threshold

---

## **Submission Instructions**

**Deadline:** Monday, 18 November @ 11:59:59 PM

* Submit through the official system.
* Only the **latest submission** will be marked.
* Keep a **screenshot** as proof of submission.
* For extension requests, apply via **myUNSW**.
* Email issues to: [yi.xu10@student.unsw.edu.au](mailto:yi.xu10@student.unsw.edu.au)

---

## **Late Submission Policy**

* **-5% per day**, up to 5 days late
* Submissions **more than 5 days late will not be accepted**

---

## **Important Notes**

* **Design an exact method** to identify similar record pairs.
  *(Refer to Week 8 lecture slides for guidance)*

* You **cannot compute all pairwise similarities directly** — that is **prohibited**

* You must use **Spark APIs only** (not regular Python code)

* During testing:

  * The code is executed using **two local threads**
  * Avoid inefficient or memory-heavy implementations

* Reference material:

  * [Efficient Parallel Set-Similarity Joins Using MapReduce (SIGMOD’10)](https://dl.acm.org/doi/10.1145/1807167.1807185)

---

## **Marking Criteria (22 Marks Total)**

### ✅ Compilation & Execution on Spark — **6 Marks**

* Your code compiles and executes correctly on Spark

### ✅ Accuracy — **6 Marks**

* All correct result pairs
* No missing or extra pairs
* Correct values and format
* Proper rounding and ordering

### ⚡ Efficiency — **10 Marks**

Efficiency is evaluated using the **runtime rank** on the largest test case (with 2 local threads). Submissions are ranked separately for correct and incorrect results.

#### If your results are correct:

$$
\text{Efficiency Score} = 10 - \left\lfloor \frac{\text{rank percentage} - 1}{10} \right\rfloor
$$

#### If your results are incorrect:

$$
\text{Efficiency Score} = 0.4 \times \left(10 - \left\lfloor \frac{\text{rank percentage} - 1}{10} \right\rfloor \right)
$$

