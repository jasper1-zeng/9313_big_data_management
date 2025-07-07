# **Assignment: Spatial and Textual Similarity Join (22 Marks)**

## **Project Description**

This task involves identifying pairs of data records that are **both geographically close** and **textually similar**. Each record contains:

* A **2D spatial coordinate** (x, y), and
* A **set of terms** representing a textual description.

The goal is to implement a system in **Apache Spark** that efficiently joins records based on both spatial and textual similarity.

---

## **Similarity Conditions**

A pair of records (R₁, R₂) qualifies as a match **only if** it satisfies **both** of the following:

### ✅ **1. Spatial Proximity**

The **Euclidean distance** between the coordinates must be within a user-defined threshold `d`:

$$
\sqrt{(R_1.x - R_2.x)^2 + (R_1.y - R_2.y)^2} \leq d
$$

### ✅ **2. Textual Similarity**

The **Jaccard Similarity** of their term sets must meet or exceed threshold `s`:

$$
\text{Jaccard}(R_1.S, R_2.S) = \frac{|R_1.S \cap R_2.S|}{|R_1.S \cup R_2.S|} \geq s
$$

---

## **Example**

### Input Dataset:

| ID | Location | Terms   |
| -- | -------- | ------- |
| 0  | (0, 0)   | a d e f |
| 1  | (4, 3)   | b c f   |
| 2  | (1, 1)   | d e f   |
| 3  | (1, 2)   | a d f   |
| 4  | (2, 1)   | b e f   |
| 5  | (5, 5)   | c e     |

With parameters:

* Distance threshold `d = 2`
* Jaccard threshold `s = 0.5`

### Expected Output:

```
(0,2):1.414214, 0.75
(2,3):1.0, 0.5
(2,4):1.0, 0.5
```

---

## **Output Format**

Each matching pair must be reported in the following format:

```
(record1_id, record2_id):distance, similarity
```

**Requirements:**

* IDs must be listed in ascending order (`id1 < id2`)
* Round both distance and similarity values to **6 decimal places**
* No duplicate pairs allowed
* Results must be sorted by the first ID, then by the second

---

## **Running the Code**

Your script should be executed as follows:

```bash
$ spark-submit project3.py input output d s
```

**Arguments:**

* `input`: Path to the input file
* `output`: Output directory path
* `d`: Distance threshold
* `s`: Similarity threshold

---

## **Submission Guidelines**

**Deadline:**
🕒 Monday, 18 November @ 11:59:59 PM

* Submit via the course submission system.
* Multiple submissions are allowed; only the last one will be marked.
* Take a screenshot as proof of submission.
* Need more time? Apply for **special consideration** through *myUNSW*.
* For help, email: [yi.xu10@student.unsw.edu.au](mailto:yi.xu10@student.unsw.edu.au)

---

## **Late Penalty**

* 5% deduction per day (up to 5 days)
* Submissions more than **5 days late will not be accepted**

---

## **Important Notes**

* You **must design a correct algorithm** for finding similar pairs (see Week 8 slides for ideas)
* You **cannot brute-force** all pairwise comparisons
* Pure Python solutions are **not allowed**
* All code is tested using **two local Spark threads**; be memory- and time-efficient
* Refer to this paper for performance tips:
  📄 [Efficient Parallel Set-Similarity Joins Using MapReduce (SIGMOD 2010)](https://dl.acm.org/doi/10.1145/1807167.1807185)

---

## **Marking Breakdown (22 Marks Total)**

| Category        | Description                                           | Marks |
| --------------- | ----------------------------------------------------- | ----- |
| **Spark Setup** | Code runs and compiles using Spark                    | 6     |
| **Correctness** | All pairs valid, complete, accurate format and values | 6     |
| **Efficiency**  | Runtime rank on large test data using 2 threads       | 10    |

### ⚡ Efficiency Grading

* If your results are **correct**:

  $$
  \text{Score} = 10 - \left\lfloor \frac{\text{rank\_percentage} - 1}{10} \right\rfloor
  $$

* If your results are **incorrect**:

  $$
  \text{Score} = 0.4 \times \left(10 - \left\lfloor \frac{\text{rank\_percentage} - 1}{10} \right\rfloor \right)
  $$