# Big Data Management Projects (COMP9313)

This repository contains three major big data processing projects developed for COMP9313 - Big Data Management course. Each project demonstrates different aspects of distributed computing and big data analytics using various frameworks and techniques.

## 🎯 Project Overview

The projects focus on text analytics and similarity computation using real-world datasets, particularly Australian Broadcasting Corporation (ABC) news headlines. Each project builds upon fundamental big data concepts while introducing increasingly complex distributed computing challenges.

---

## 📊 Project 1: Top-k Term Weights Computation (12 Marks)

### **Objective**
Compute the top-k terms with the largest TF-IDF weights for each year from ABC news headlines using **MapReduce**.

### **Technology Stack**
- **Framework**: MRJob (MapReduce)
- **Platform**: Hadoop
- **Language**: Python 3

### **Key Features**
- **TF-IDF Weight Calculation**: Implements Term Frequency-Inverse Document Frequency scoring
- **Efficient Top-k Selection**: Uses sorting and filtering to identify most important terms
- **Combiner Implementation**: Reduces data transfer with in-mapper combining
- **Order Inversion**: Implements custom keys for efficient sorting
- **Secondary Sort**: Ensures proper ordering by year, weight, and term

### **Input Format**
```
date,term1 term2 term3 ...
```
Example:
```
20191124,woman stabbed adelaide shopping centre
20191204,economy continue teetering edge recession
```

### **Output Format**
```
"Year"    "Term,Weight"
```
Example:
```
"2019"    "adelaide,0.3010299956639812"
"2019"    "centre,0.3010299956639812"
```

### **Usage**
```bash
python3 project1.py -r hadoop input_file -o hdfs_output \
--jobconf myjob.settings.k=2 \
--jobconf mapreduce.job.reduces=2
```

---

## 🔍 Project 2: Average Term Weights Computation (16 Marks)

### **Objective**
Compute yearly average term weights and identify top-k important terms using **Apache Spark**, implementing both RDD and DataFrame APIs.

### **Technology Stack**
- **Framework**: Apache Spark
- **APIs**: RDD and DataFrame
- **Language**: Python 3

### **Key Features**
- **Dual Implementation**: Both RDD and DataFrame solutions
- **Stop Words Filtering**: Removes top-n most frequent terms
- **Average Weight Calculation**: Computes mean TF-IDF weights across years
- **Efficient Processing**: Uses Spark's distributed computing capabilities

### **Algorithm**
1. **TF Calculation**: `TF(term t, year y) = log10(frequency of term t in year y)`
2. **IDF Calculation**: `IDF(term t, year y) = log10(number of headlines in year y / number of headlines in year y that contain term t)`
3. **Weight**: `Weight(term t, year y) = TF(t, y) * IDF(t, y)`
4. **Average**: Compute mean weights across all years per term

### **Input/Output**
- **Input**: Same format as Project 1
- **Output**: `term\tWeight` format

Example output:
```
"session"	1.496589902
"fire"	1.425714596
"saddam"	1.421863837
```

### **Usage**
```bash
# RDD Implementation
spark-submit project2_rdd.py "file:///home/abcnews.txt" "file:///home/output" 1 5

# DataFrame Implementation  
spark-submit project2_df.py "file:///home/abcnews.txt" "file:///home/output" 1 5
```

**Parameters:**
- Input file path (with `file:///` prefix for local files)
- Output folder path  
- `n` = number of most frequent terms to ignore
- `k` = number of top terms to output

---

## 🔍 Project 3: Spatial and Textual Similarity Join (22 Marks)

### **Objective**
Perform similarity joins based on both spatial proximity and textual similarity using **Apache Spark**.

### **Technology Stack**
- **Framework**: Apache Spark
- **APIs**: RDD
- **Language**: Python 3
- **Algorithms**: Spatial indexing, Jaccard similarity

### **Key Features**
- **Dual Similarity Metrics**: Combines Euclidean distance and Jaccard similarity
- **Spatial Grid Indexing**: Efficient spatial partitioning for proximity search
- **Prefix Filtering**: Reduces candidate pairs using textual prefixes
- **Exact Algorithm**: Implements precise similarity join without approximation

### **Similarity Criteria**
Records must satisfy both conditions:
1. **Euclidean Distance**: `√[(R₁.x - R₂.x)² + (R₁.y - R₂.y)²] ≤ d`
2. **Jaccard Similarity**: `|R₁.terms ∩ R₂.terms| / |R₁.terms ∪ R₂.terms| ≥ s`

### **Input Format**
```
id#(x,y)#term1 term2 term3 ...
```
Example:
```
0#(0,0)#a d e f
1#(4,3)#b c f
2#(1,1)#d e f
```

### **Output Format**
```
(record1.id,record2.id):distance, similarity
```
Example:
```
(0,2):1.414214, 0.75
(2,3):1.0, 0.5
(2,4):1.0, 0.5
```

### **Usage**
```bash
spark-submit project3.py input output d s
```

**Parameters:**
- `input`: path to input file
- `output`: output folder path
- `d`: distance threshold
- `s`: similarity threshold

---

## 📁 Repository Structure

```
24-t3-comp9313-big-data-management/
├── P1/                          # Project 1: MapReduce TF-IDF
│   ├── project1.py             # Main implementation
│   ├── abcnews.txt            # Sample dataset
│   ├── Project 1 description.md
│   └── test.txt               # Test data
├── P2/                          # Project 2: Spark Average Weights
│   ├── final_sub/             # Final submissions
│   │   ├── project2_rdd.py    # RDD implementation
│   │   ├── project2_df.py     # DataFrame implementation
│   │   ├── result_n=5_k=5.txt # Sample results
│   │   └── abcnews.txt        # Dataset
│   ├── Project 2 description.md
│   └── [development versions]
├── P3/                          # Project 3: Similarity Join
│   ├── final_sub/             # Final submissions
│   │   ├── project3.py        # Main implementation
│   │   ├── result_d=12_s=0.2.txt # Sample results
│   │   └── [test files]
│   ├── project_3_description.md
│   └── [development versions]
└── README.md                    # This file
```

## 🚀 Getting Started

### Prerequisites
- **Python 3.x**
- **Apache Hadoop** (for Project 1)
- **Apache Spark** (for Projects 2 & 3)
- **MRJob library** (for Project 1)

### Installation
```bash
# Install MRJob for Project 1
pip install mrjob

# Spark should be installed and configured
# Set SPARK_HOME and add to PATH
```

## 📈 Results Summary

### Project 1 Results
- Successfully computes TF-IDF weights using MapReduce
- Implements efficient top-k selection with proper ordering
- Handles large datasets with multiple reducers

### Project 2 Results  
- **Sample Output** (n=5, k=5):
  - "session": 1.496589902
  - "fire": 1.425714596  
  - "saddam": 1.421863837

### Project 3 Results
- **Sample Output** (d=12, s=0.2):
  - (7,14035): distance=10.316295, similarity=0.222222
  - (27,7285): distance=7.179233, similarity=0.25
  - 538 total matching pairs found

## 🎓 Learning Outcomes

Through these projects, key big data concepts were explored:

1. **Distributed Computing**: MapReduce and Spark paradigms
2. **Text Analytics**: TF-IDF, similarity measures, information retrieval
3. **Spatial Computing**: Grid-based indexing, proximity search
4. **Algorithm Optimization**: Efficient joins, filtering, and aggregation
5. **Big Data Tools**: Hadoop ecosystem, Spark RDD/DataFrame APIs

## 📝 Course Information

- **Course**: COMP9313 - Big Data Management
- **Institution**: UNSW Sydney
- **Term**: 24T3 (Term 3, 2024)

---

*This repository demonstrates practical applications of big data processing techniques for real-world text analytics and spatial computing challenges.*
 
