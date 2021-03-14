# Text Corpus Analysis
Vocabulary Exploration to produce few statistics like the number of common words, unique words to the data sets and finding the percentage of words appearing in five genres, in four genres, in three genres, in two genres and in one genre with and without including a list of stop words.
Sentence Vector Exploration to compare two sentence vector representation methods based on their ability to captures the genre feature of sentences. These include using TDIDF based vector representation by Spark ML library and pre-trained sentence encoder released by Google.
# Setting the Environment
# Creating the EMR cluster
```
Login to AWS console and proceed to S3 dashboard. Create a bucket comp5349-auda0496 with a folder assignment and upload the datasets from the MNLI data set. The files to be uploaded are:
*train.tsv: the training data set
*dev_matched.tsv: the development set with matched genre
*dev_mismatched.tsv : the development set with mismatched genre
*test_matched.tsv: the test set with matched genre
*test_mismatched.tsv: the test set with mismatched genre
*Also download stop_words.txt from the Stanford Stop Word List and upload it to the storage.

```

* Proceed to EMR dashboard and create a cluster. Proceed with advanced options link right next to quick options and enter the advanced options page.

* First in the software and steps configuration, select EMR release 5.29 from the drop-down list. Then click Hadoop 2.8.5, Spark 2.4.4 Livy 0.6.0 and Tensorflow 1.14.0 to include the four components. In Edit Software Settings section, paste the following configuration into the provided text box. This is the configuration to enable maximum resource allocation

```
[{"classification":"spark","properties":{"spark.executor.memory":"1g","spark.driver.memory":"14000M","maximizeResourceAllocation":"true","spark.driver.memoryOverhead":"512","spark.executor.memoryOverhead":"512"}},{"classification":"yarn-site","properties":{" yarn.nodemanager.vmem-pmem-ratio":"4","yarn.nodemanager.pmem-check-enabled":"false","yarn.nodemanager.vmem-check-enabled":"false"}}]
```

```
Next in the hardware configuration select 1 master node and 4 core nodes. Also select m5.2xlarge instance type for the master node and m4.xlarge instance type for the core nodes.
Expand the Bootstrap Actions options and in the Add bootstrap action drop down list, click on “Custom action”, then click Configure and Add. Upload a script with the commands given below to S3 and using the window select the script to add it.
sudo python3 -m nltk.downloader -d /usr/local/share/nltk_data all
sudo pip-3.6 install --quiet tensorflow-hub
sudo pip-3.6 install --quiet matplotlib
Next in security add your EC2 keypair and launch the cluster.
```
#Adding the EMR Notebook
After the EMR cluster is created go to the dashboard and click Notebooks on the left-hand side menu. Click “Create Notebook”, give it a name and in clusters section click “Choose an existing cluster” and select the cluster you have just created.
Choose the bucket folder created in S3 as the location to store the notebook. Once the notebook is created the status will change from “Starting” to “Pending” to “Ready”. When ready open the notebook using Jupyter and here upload the .ipynb file and open it. Check and change the kernel to Pyspark and now the program can be run.

#Imported Packages and Libraries
from pyspark SparkConf, SparkContext
from pyspark.sql SparkSession
from pyspark.streaming StreamingContext
nltk
from nltk.tokenize word_tokenize
from nltk.corpus stopwords
tensorflow
tensorflow_hub
numpy
