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
docker cp twitter.csv namenode:twitter.csv
docker exec -it namenode bash
```

## Hadoop
### Create a HDFS directory
```
hdfs dfs -mkdir -p /data/covid/
```
### Copy twitter data to HDFS.
```
hdfs dfs -put twitter.csv /data/covid/twitter.csv
```


## Spark
### open scala spark shell 
```
docker exec -it spark-master bash
spark/bin/spark-shell --master spark://spark-master:7077
```

### load data from hdfs
 ```
val twitterDF = spark.read.csv("hdfs://namenode:9000/data/covid/twitter.csv")

val covDF = spark.read.csv("hdfs://namenode:9000/data/covid/covid-hospitalizations.csv")
 ```
 ### clean up
 ```
 val cleanTwitterDF = twitterDF.dropDuplicates
 val cleanCovDF = covDF.drop("_c1")
 ```

### filtering for countries
 ```
 val twitterDF_clean = twitterDF.dropDuplicates
 val test = twitterDF.filter(twitterDF("_c2") === "22/3/2020")

 val covDF_USA = covDF.filter(covDF("_c0") === "United States")
 val covDF_ITA = covDF.filter(covDF("_c0") === "Italy")
 val covDF_FRA = covDF.filter(covDF("_c0") === "France")
 val covDF_GER = covDF.filter(covDF("_c0") === "Germany")
 val covDF_ENG = covDF.filter(covDF("_c0") === "England")
 ```
### filtering for intensive care unit numbers in 2020
 ```
 val covDF_USA_icu = covDF_USA.filter(covDF_USA("_c3") === "Daily ICU occupancy" && (covDF_USA("_c2") rlike "2020-*"))
 val covDF_ITA_icu = covDF_ITA.filter(covDF_ITA("_c3") === "Daily ICU occupancy" && (covDF_ITA("_c2") rlike "2020-*"))
 val covDF_FRA_icu = covDF_FRA.filter(covDF_FRA("_c3") === "Daily ICU occupancy" && (covDF_FRA("_c2") rlike "2020-*"))
 val covDF_GER_icu = covDF_GER.filter(covDF_GER("_c3") === "Daily ICU occupancy" && (covDF_GER("_c2") rlike "2020-*"))
 val covDF_ENG_icu = covDF_ENG.filter(covDF_ENG("_c3") === "Daily ICU occupancy" && (covDF_ENG("_c2") rlike "2020-*"))

 ```
 ### basic descriptive statistics
```
 covDF_USA_icu.describe().show
 covDF_ITA_icu.describe().show
 covDF_FRA_icu.describe().show
 covDF_GER_icu.describe().show
 covDF_ENG_icu.describe().show
```

### casting columns to Integers
```
val cleanTwitterDF2= cleanTwitterDF1.withColumn("_c1",col("_c1").cast("Integer"))
val hospitalDF2 = covDF.withColumn("_c4",col("_c4").cast("Integer"))
```

### computing daily numbers
Computation of worldwide ICU occupancy per day and the number of mentions of the word "coronavirus" per day.
```
var day = 22
var month = 3
var data = Seq((1,1))

for( i <- 1 to 20){
    var d = ""
    var m = ""
    if(day<10){
        d = "0".concat(day.toString)
    } else {
        d = day.toString
    }
    if(month<10){
        m = "0".concat(month.toString)
    } else {
        m = month.toString
    }
    val date = "2020-".concat(m).concat("-").concat(d)
    val date2Format = day.toString.concat("/").concat(month.toString).concat("/2020")

    val filtered4 = hospitalDF2.filter(hospitalDF2("_c2") === date && hospitalDF2("_c3") === "Daily ICU occupancy")
    val a = filtered4.rdd.map(x => (1,x.getInt(4))).reduceByKey(_ + _).collect()
    val sum_ICU = a(0)._2

    val wordFiltered = cleanTwitterDF2.filter(cleanTwitterDF2("_c2") === date2Format 
    											&& cleanTwitterDF2("_c0") === "coronavirus")
    val mapp = wordFiltered.rdd.map(x => (1,x.getInt(1))).collect()
    val sum_WC = mapp(0)._2
    data = data :+ (sum_ICU,sum_WC)

    day +=1
    if(day == 31 && month != 3 && month != 5 && month != 7 && month != 8 && month != 10) {
        day = 1
        month += 1
    } else if(day == 32){
        day = 1
        month += 1
    }
}
```

### create dataframe out of the computed tuples
```
val columns = Seq("Total_ICU_PerDay","wordCount_PerDay")
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD = spark.createDataFrame(rdd).toDF(columns:_*)

var df = dfFromRDD.withColumn("Total_ICU_PerDay",col("Total_ICU_PerDay").cast("Integer"))
var df2 = df.withColumn("wordCount_PerDay",col("wordCount_PerDay").cast("Integer"))

```

### calculating correlation coefficient
```
val pearsonCoefficient = df2.stat.corr("Total_ICU_PerDay","wordCount_PerDay")
```
