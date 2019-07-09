# InfectionWatch

# Introduction

The aim of this project is to build a cloud based platform to automatic collect, analyze and store healthcare data for clinicians and researchers to jointly analyze and predict infectious disease outbreaks. 
[View application](rtinfo-insight.com) or [watch demo](rtinfo-insight.com)


# Motivation
Infectious diseases are associated with substantial morbidity, mortality and cost. Manual identification of infections is costly, time-consuming and diverts staff time from prevention activities. Although next-generation DNA sequencing has demonstrated its ability to identify microbial pathogens where traditional diagnostics have otherwise failed, there are several limitations to use it for infection detection in clinical setting: 
1. The volume of data that is produced from next-generation sequencing platforms is massive. 
2. Data analysis is time-consuming and requires sophisticated bioinformatics systems. 
3. The lack of computational resources for large dataset storage and management limits capability to analyze and clinically interpret the data.


# Tools and technologies used 
1. AWS S3
2. Apache Spark
3. Airflow
4. AWS Redshift
5. Flask 
6. Tableau

Flow of the data
------------------------------------

![Alt text](pipeline.png?raw=true "Optional Title")

# Details of the workflow

> ***Data source*** 
All the raw data used for processing and generating diagnosis report is from NIH Human Microbiome Project and saved in AWS S3 bucket (total 6TB). The [NIH Human Microbiome Project](https://portal.hmpdacc.org/search/s?facetTab=files&filters=%7B%22op%22:%22and%22,%22content%22:%5B%7B%22op%22:%22in%22,%22content%22:%7B%22field%22:%22files.file_format%22,%22value%22:%5B%22FASTA%22,%22FASTQ%22%5D%7D%7D%5D%7D) performed whole metagenomic shotgun sequencing (mwgs) on over 1200 samples collected from 15-18 body sites from 300 human subjects. They provide access to raw mwgs sequence data in fastq format.

> ***Data process***
This cloud-based platform contains two pipelines. The first pipeline collects and analyzes large-scale genomic datasets to generate diagnosis for infection automatically.The other pipeline is designed as distributed data mining systerm to review and process millions of patients’ medical records efficiently. 
These pipelines utilized Spark and Redshift to process raw data and build a data warehouse for designing individualized treatment strategies. By elastically scaling the number and types of compute instances – in addition to optimization of pipelines to run in parallel – there are no limits to the amount of data you can analyze. 

> ***Airflow jobs***
A scheduler would be running to collect the data from S3 to Spark jobs every two hours. The raw sequencing data is collected and processed to get accurate diagnosis for infection. At the sametime, detailed healthcare information is processed and saved to data warehouse. All those data is further analyzed and posted to the dashboard dynamicaly. 

> ***UI design***
Two-tier data sharing application for both public health-care management and clinical report dashboard allowing clinicians to better protect patients by identifying and investigating potential clusters of infections.
Daily reports will be generated from the collected data. A detailed summary of infection type, patient's metadata and previous medical records would be available to the doctors for further analysis.

### Environment Setup

Install and configure [AWS CLI](https://aws.amazon.com/cli/) and [Pegasus](https://github.com/InsightDataScience/pegasus) on your local machine, and clone this repository using
`git clone https://github.com/jing0703/InfectionWatch.git`.

> AWS Tip: Add your local IP to your AWS VPC inbound rules

> Pegasus Tip: In $PEGASUS_HOME/install/download_tech, upgrade pip and python, and follow the notes in docs/pegasus_setup.odt to configure Pegasus

#### CLUSTER STRUCTURE:

To reproduce my environment, 10 m4.large AWS EC2 instances are needed:
- (4 nodes) Spark Cluster 1 - Batch for DNA sequencing data analysis
- (4 nodes) Spark Cluster 2 - Batch for medical records data processing
- Flask Node
- Airflow Node

To create the clusters, put the appropriate `master.yml` and `workers.yml` files in each `cluster_setup/<clustername>` folder (following the template in `cluster_setup/dummy.yml.template`), list all the necesary software in `cluster_setup/<clustername>/install.sh`, and run the `cluster_setup/create-clusters.sh` script.

> After that, **InfectionWatch** will be cloned to the clusters (with necessary .jar files downloaded and required Python packages installed), and any node's address from cluster *some-cluster-name* will be saved as the environment variable SOME_CLUSTER_NAME_$i on every master, where $i = 0, 1, 2, ...*


##### Airflow setup
The Apache Airflow scheduler can be installed on the master node of *spark-batch-cluster*. Follow the instructions in `docs/airflow_install.txt` to launch the Airflow server.

##### AWS Redshift setup
Follow the instruction from [AWS Guide](https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html) to setup cluster.
Detailed information for creating tables, uploading data, and querying the database in Redshift cluster can not found in 

##### Install other tools
Download and install [Biopython] (https://biopython.org/wiki/Download) and [Sparkhit] (https://rhinempi.github.io/sparkhit/example.html) in all your Spark Cluster nodes for DNA data analysis.
Download and install [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html) in all your Spark Cluster nodes.

##### Configurations
Configuration settings for Spark tasks and AWS S3 bucket are stored in the respective files in `config/` folder.
> Replace the settings in `config/s3config.ini` with the names and paths for your S3 bucket.

## Running InfectionWatch

### Schedule the Batch Job
Running `airflow/schedule.sh` on the node of *airflow-instance* will add the batch job to the scheduler. The batch job is set to execute every 2 hours, and it can be started and monitored from the Airflow GUI at `http://$SPARK_BATCH_CLUSTER_0:8081`.

### Start the Spark batch Job
Execute `./allignment.sh` on the master of *spark-batch-cluster-1* for DNA sequencing analysis;
Execute `./transformation.sh` on the master of *spark-batch-cluster-2* for medical records analysis (preferably after setting up AWS credentials to access S3 and write to Redshift).

### Flask and Tableau
Run `flask/app.py` to start the Flask server. The Tableau dashboard has been embeded in the Flask dashboard.

### Possible extension
Integrated data warehousing system used in this platform could be used for facilitating the integration of artificial intelligence into data mining. Both the hisotrical and the incoming healthcare data can be used with machine learning algorithms for automation of infection detaction and prediction. This pipleline is also to ingest voluminous and incremental datasets and complex data analytics methods for bioinformatics and genomics research.

