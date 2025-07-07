Here is the **original (plain text) version** of the assignment description, exactly as requested:

---

**Top-k Term Weights Computation (12 marks)**

**Background:** Detecting popular and trending topics from news articles is important for public opinion monitoring. In this project, your task is to analyze text data over a dataset of Australian news from ABC (Australian Broadcasting Corporation) using MRJob. The problem is to compute the top-k terms with the largest weights for each year in the news articles dataset.

**Input file:** The dataset you will use contains data from news headlines published over several years. In this text file, each line is a headline of a news article, in the format of "date,term1 term2 ... ... ". The date and text are separated by a comma, and the terms are separated by a space character. A sample file is like the one below (note that the stop words like “to”, “the”, and “in” have already been removed from the dataset):

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

When you click the panel on the right you'll get a connection to a server that has, in your home directory, a text file called "abcnews.txt", containing some sample text (feel free to open the file and explore its contents). The entire dataset can be downloaded from [https://www.kaggle.com/therohk/million-headlines](https://www.kaggle.com/therohk/million-headlines).

**Term weights computation:** To compute the weight for a term regarding a year, please use the TF/IDF model. Specifically, the TF and IDF can be computed as:

```
TF(term t, year y) = the frequency of t in y  
IDF(term t, year y) = log10(the number of headlines in y / the number of headlines in y having t)  
```

Please import math and use `math.log10()` to compute the term weights.

Finally, the term weight of term t in year y is computed as:

```
Weight(term t, year y) = TF(term t, year y) * IDF(term t, year y)
```

**Problem:** Your task is to compute the term weights in each year and find the top-k terms with the highest weights for each year.

If N years exist in the dataset, you should output up to N × K lines in your final output file on HDFS. (If a certain year has fewer than k terms, output all available terms and weights for that year.)

In each line, you need to output the year, the term, and its weight, in the format of:

```
"Year"\t"Term,Weight"
```

Your results should be ranked by the year in ascending order first, then by the weights in descending order, and finally by the term alphabetically.

**For example**, given the above data set and k=2, the output should be (there is no need to remove the quotation marks that MRJob generates):

```
"2019"    "adelaide,0.3010299956639812"  
"2019"    "centre,0.3010299956639812"  
"2020"    "aid,0.6020599913279624"  
"2020"    "apartment,0.6020599913279624"  
"2021"    "bounce,0.47712125471966244"  
"2021"    "china,0.47712125471966244"  
```

**Code Format:** The code template has been provided. We will use more than 1 reducer to test your code. Assuming k=2, and we use 2 reducers, we will use the following command to run your code:

```
$ python3 project1.py -r hadoop input_file -o hdfs_output --jobconf myjob.settings.k=2 --jobconf mapreduce.job.reduces=2
```

**Note:** You can access the value of `k` in your program like:

```python
from mrjob.compat import jobconf_from_env  
y = jobconf_from_env('myjob.settings.k')
```

---

### **Submission**

* **Deadline:** Wednesday 09th October 11:59:59 PM
* If you need an extension, please apply for a special consideration via “myUNSW” first.
* You can submit multiple times before the due date and we will only mark your final submission.
* To prove successful submission, please take a screenshot as the assignment submission instructions show and keep it to yourself.
* If you have any problems with submissions, please email `yi.xu10@student.unsw.edu.au`.

---

### **Late Submission Penalty**

* 5% reduction of your marks for up to 5 days
* Submissions delayed for over 5 days will be rejected

---

### **Marking Criteria**

| Criteria                                                        | Marks                                                          |
| --------------------------------------------------------------- | -------------------------------------------------------------- |
| Code compiles and runs on Hadoop in Ed environment              | +4                                                             |
| All term weight values in the output are correct                | +1                                                             |
| The order in the output is correct                              | +1 (Note: Only guarantee the order within each reducer output) |
| The efficiency of the top-k selection                           | +1                                                             |
| Correctly implements the combiner or in-mapper combining        | +1                                                             |
| Correctly implements order inversion (i.e., using special keys) | +1                                                             |
| Correctly implements secondary sort                             | +1                                                             |
| Can produce the correct result using a single MRStep            | +1                                                             |
| Can produce the correct result with multiple reducers           | +1                                                             |

---

> ❗ Submissions only containing regular Python techniques will be marked **0**.

> ❗ You **cannot** simply emit all mapper input key-value pairs and buffer them in memory on reducers — such a method will receive **no more than 4 marks**.

> ❗ Submissions that cannot be compiled and run on Hadoop in the Ed environment will receive **no more than 4 marks**.

---

Let me know if you'd like help implementing this in MRJob or testing it in your Hadoop environment.
