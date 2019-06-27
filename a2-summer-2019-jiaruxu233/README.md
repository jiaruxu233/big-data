# Assignment - Pig and Hive (10 points)

**You should thoroughly read through the entire assignment before beginning your work!**


## Cluster Setup

Create an EMR cluster with **_Advanced Options_** and the following configuration:

* Select `emr-5.23.0` from the drop-down
* **Click check-boxes for these applications only**: Hadoop 2.8.5, Hive 2.3.4, Pig 0.17.0
* Click Next
* Edit the instance types and set 1 master and 4 core of m4.large 
* Click Next
* Give the cluster a name, and you can uncheck logging, debugging and termination protection enabled
* Click Next
* Select your key-pair
* Click "Create Cluster"

Once the cluster is in "Waiting" mode (should only take a few minutes), ssh into the master **with agent forwarding:**

```
ssh-add
ssh -A hadoop@...
```

**Run the following commands, making sure to replace the values for `[[your-name]], [[your-email]] and [[your-assignment-repository]]` for the appropriate values.**

```
sudo yum install -y git
git config --global user.name [[your name]]
git config --global user.email [[your email]]
git clone [[your-assignment-repository]]
cd [[repository-directory]]
```
### Reminders

* all files must be within the repository directory otherwise git will not see them.
* commit and push back to GitHub as you are doing your work. **If you terminate the cluster and you did not push to GitHub, you will lose all your work.**
* data in the cluster's HDFS will be lost when the cluster terminates. If you want to keep data, store it in S3.


## Provide the Master Node and Cluster Metadata

Once you are ssh'd into the master node, query the instance metadata and write to a file:

```
curl http://169.254.169.254/latest/dynamic/instance-identity/document/ > instance-metadata.json
```

Also, since you are using a cluster, please provide some metadata files about your cluster. Run the following commands:

```
cat /mnt/var/lib/info/instance.json > master-instance.json
cat /mnt/var/lib/info/extraInstanceData.json > extra-master-instance.json
```



## Problem - Analyzing data with Pig and Hive

In this assignment, you will be working with a large subset of a much larger dataset. The dataset you will be using is called the Google Ngram Dataset. You can read more about it here: [http://storage.googleapis.com/books/ngrams/books/datasetsv2.html](http://storage.googleapis.com/books/ngrams/books/datasetsv2.html)

The 2-grams, or bigrams dataset has been obtained from Google, unzipped, and placed in the `s3://bigdatateaching/bigrams/` bucket. You can read what a bigram is here: [https://en.wikipedia.org/wiki/Bigram](https://en.wikipedia.org/wiki/Bigram). This dataset is 30,865,914,791 (yes - 30 billion) records and is about 780 GiB (gigabytes.) 

Each line in every file has the following format:

`n-gram TAB year TAB occurrences TAB books`

An example of a set of fictitious records is:

```
I am	1936	342	90
I am	1945	211	10
very cool	1923	500		10
very cool	1980 	3210	1000
very cool	2012	9994	3020
```

This tells us that in 1936, the bigram `I am` appeared 342 times in 90 different books. In 1945, `I am` appeared 211 times in 10 different books. Same for `very cool`: in 1923, it appeared 500 times in 10 books, etc.


### Analysis to perform on the dataset

**Since the total bigram set is significantly large, for this assignment you will only be working with the bigram files for specific letters. See below for details.**  

### Copying files in bulk from S3 to HDFS

The bigram files are all contained within a single directory in S3. This works fine when working with Pig because you can use wildcards when loading Pig files. However, when using Hive, Hive only works with an entire directory on S3.

To overcome this limitation, you will copy the files you need to work with  from S3 to HDFS. You can then specify either individual files or the entire directory when working in Hive and Pig. 

For example, to copy the "a" files from S3 to a directory in your cluster's HDFS, follow the following steps:

* From the command line in the Master node, create a directory in HDFS called `a-bigrams`:	`hadoop fs -mkdir a-bigrams`

* Run this command which runs a parallel copy (it is actually a Hadoop job) to  copy the "a" files from S3 to HDFS:

	`hadoop  distcp s3://bigdatateaching/bigrams/googlebooks-eng-us-all-2gram-20120701-a? a-bigrams/`

### Analysis

You will be performing some calculations on this dataset. For every unique bigram, you will create an aggregated dataset with the following calculated fields:

* Total number of occurrences
* Total number of books
* The average occurences per book for all years that the bigram has been documented.For the example above, the calculation will be the following:

```
I am:         (342 + 211) / (90 + 10) = 5.53
very cool     (500 + 3210 + 9994) / (10 + 1000 + 3020) = 3.40049628
```
* The first year that bigram appeared in the data
* The last year the bigram appeared in the data
* The total number of years that the bigram appeared in the date (note: they may or may not appear every year)

For the input date above, your aggregated dataset would look something like this:

```
I am	553	100	5.53	1936	1945	2
very cool	13704	4030	3.40049628	1923	2012	4		
```

From this aggregated dataset, output the **top 50 bigrams with the highest average number of occurrences per book** along with all the fields calculated that meet the following criteria:

* Bigram must have first appeared in 1950
* Bigram must have appeared **all** years between 1950 and 2009 (the last year of data is 2009)

Store the output in a **csv (comma separated)** file. All fields (7 fields) must be in the results dataset. No field header is needed.

Couple of things to keep in mind:

* Remember to filter early and often
* If you want to show output in your screen use the LIMIT command (in PIG) and select only a few records in Hive
* When developing your scripts you may want to use a single file in your LOAD command (in Pig) and when creating the external table in Hive. Pick one of the files for the specific letter to develop on. Once you are ready to run the whole set of files for a particular letter, you can use all of them specifying the directory

### Requirements for the Pig and Hive scripts

Write a Pig script and a Hive script that perform the analysis explained above. 

**Requirements for the Pig script only**

* The Pig script will perform the analysis on the "i" files only (~31 GB)
* Output must be sorted by the average number of occurrences per book in descending order

**Requirements for the Hive script only**

* The Hive script will perform the analysis on the "r" files only (~28 GB)
* Output must be sorted by the average number of occurrences per book in ascending order


**Requirements for both scripts**

* Scripts must read the required letter files
* Scripts must create the aggregated dataset

These scripts need to be called `pig-script.pig` and `hive-script.hql`.

Remember that when you store the outputs of Pig/Hive, this output is inside HDFS or S3. You then need to extract the file(s) from HDFS or S3 into the master node's filesystem so you can add to your repository. **You must write two separate scripts and run them.**

The results files need to be called `pig-results.csv` and `hive-results.csv`.


## Submitting the Assignment

Make sure you commit **only the files requested**, and push your repository to GitHub!

The files to be committed and pushed to the repository for this assignment are:

* `instance-metadata.json`
* `master-instance.json`
* `extra-master-instance.json`
* `pig-script.pig` (your Pig script)
* `pig-results.csv` 
* `hive-script.hql` (your Hive script)
* `hive-results.csv` 



## Grading Rubric

Pig and Hive scripts are worth 3 points each. Output files are worth 2 points each.

-   We will look at the results files and/or scripts. If the result files are exactly what is expected, in the proper format, etc., we may run your scripts to make sure they produce the output. If everything works, you will get full credit for the problem.
-   If the submitted results are not what is expected, we will look at and run your code and provide partial credit wherever possible and applicable.
-   Points **will** be deducted for each the following reasons:
    -   Instructions are not followed
    -   Output is not in expected format (not sorted, missing fields, wrong delimiter, unusual characters in the files, etc.)
    -   There are more files in your repository than need to be
    -   There are additional lines in the results files (whether empty or not)
    -   Files in repository are not the requested filename
    -   Homework is late 



	