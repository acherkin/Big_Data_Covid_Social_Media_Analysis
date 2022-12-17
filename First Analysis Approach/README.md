# Big_Data_Covid_Social_Media_Analysis
Analyzes how covid related posts correspond to world covid test and hospitalization data

Reddit Dataset:
https://socialgrep.com/datasets/the-reddit-covid-dataset

Twitter Dataset:
https://www.kaggle.com/datasets/imoore/covid19-complete-twitter-dataset-daily-updates?select=dailies

Covid Tests Dataset: 
https://github.com/owid/covid-19-data/tree/master/public/data/testing

Hospitalizations Dataset:
https://github.com/owid/covid-19-data/tree/master/public/data/hospitalizations

-------------------------------
Dev Resources

Docker images: https://github.com/Marcel-Jan/docker-hadoop-spark


-------------------------------


# Used Commands

## Docker
```
sudo docker-compose up
sudo docker-compose down
sudo docker container ls
```
### copying data to docker
```
docker cp [file name] namenode:[filename]
docker exec -it namenode bash
```

## Hadoop
### Create a HDFS directory
```
hdfs dfs -mkdir -p /data/
```
### Copy twitter data to HDFS.
```
hdfs dfs -put [file name] /data/[file name]
```
###Run MapReduceDaily
```
hadoop jar FinalProjectLoadData.jar
```
###If upload unseccessful
###Run the CountTweets.java file on host (src > main > java > Backup > CountTweets.java)
###May need to modify paths. Make sure the .tsv covid post data is in one directory.
###Do not use the clean dataset.
###After, a .csv file is generated

###Upload to docker
###Upload to hdfs


## Spark
### open pyspark spark shell 
```
docker exec -it spark-master bash
  /spark/bin/pyspark --master spark://spark-master:7077
```

### All commands for pyspark processing can be found in CovidToPostProcessing.ipynb and can be entreted into the shell (please ignore the output in the notebook)
