# Assignment - Spark RDD and SparkSQL

**You should thoroughly read through the entire assignment before beginning your work! Don't start the cluster until you are ready.**

## Cluster Setup

Create an EMR cluster with **_Advanced Options_** and the following configuration:

* Select `emr-5.24.0` from the drop-down
* **Click check-boxes for these applications only**: Hadoop 2.8.5 and Spark 2.4.2
* Click Next
* Edit the instance types and set 1 master and 4 core of m4.xlarge 
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

**Note: you only need to run the `post-startup-configuration.sh` script once. If you run it more than once, it will mess up the environment.**

Once this is done, please type `exit` to logoff and then log back on, making sure you enable both ssh-agent forwarding and port forwarding:

```
ssh-add
ssh -A -L8765:localhost:8765 hadoop@...
``` 

You can then open a browser and navigate to [http://localhost:8765](http://localhost:8765) to see your Jupyter Notebook environment, which got started within the assignment directory. 

### Provide the Master Node and Cluster Metadata

Provide the instance metadata. Run this command in the master node command line.

```
curl http://169.254.169.254/latest/dynamic/instance-identity/document/ > instance-metadata.json
```


## Problem 1: Working with RDDs (4 points)

Amazon maintained a list of the top 1 million Internet sites by traffic at the URL (they do not anymore). A recent copy is included in this repository which you will use for this problem. 

In this problem you will:

* Make an RDD where each record is a tuple with the (rank, site)
* Determine the number representation of top-level domains (TLDs) in the top 10,000 websites. Example TLDs are `com`, `edu` and `cn`. (Do not include the `.`). 

### Place the domains file in HDFS

* Unzip the file `top-1m.csv.zip`
* Put the file `top-1m.csv` into HDFS with the command:

```
[hadoop@ip-172-31-91-222 ~]$ hadoop fs -put top-1m.csv top-1m.csv
```

Verify that the file is there with `hadoop fs -ls`.

```
[hadoop@ip-172-31-91-222 ~]$ hadoop fs -ls
Found 2 items
drwxr-xr-x   - hadoop hadoop          0 2018-03-25 18:48 .sparkStaging
-rw-r--r--   2 hadoop hadoop   23245165 2018-03-25 18:50 top-1m.csv
```

The rest of the work will be done within the [problem-1.ipynb](problem-1.ipynb) Jupyter notebook.

After you finish working on the problem, you will commit the Jupyter notebook `.ipynb` file called `problem-1.ipynb.`

## Problem 2: Working with SparkSQL (6 points)

You will perform the work for this problem within the [problem-2.ipynb](problem-2.ipynb) Jupyter notebook.

After you finish working on the problem, you will commit the Jupyter notebook `.ipynb` file called `problem-2.ipynb.`

## Submitting the Assignment

Make sure you commit **only the files requested**, and push your repository to GitHub!

The files to be committed and pushed to the repository for this assignment are:

* `instance-metadata.json`
* `problem-1.ipynb`
* `problem-2.ipynb`


## Grading Rubric 

* We will look at the results files and the scripts. If the result files are exactly what is expected, in the proper format, etc., we may run your scripts to make sure they produce the output. If everything works, you will get full credit for the problem.
* If the results files are not what is expected, or the scripts produce something different from what is expected, we will look at code and provide partial credit where possible and applicable.
* Points will be deducted for each the following reasons:
	* Instructions are not followed
	* Output is not in expected format (not sorted, missing fields, wrong delimeter, unusual characters in the files)
	* There are more files in your repository than need to be 
	* There are additional lines in the results files (whether empty or not)
	* Files in repository are not the requested filename
