package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static javax.print.attribute.standard.Compression.COMPRESS;


public class MapReduceDaily {
    /*
    One Map-Reduce job for Query 2: tot number of transactions and transTotals. Used in-map join to get customer names. Combiner used for sums.
     */

    public static class Mapper1
            extends Mapper<Object, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);
        //HashMap<String, String> userData = new HashMap<String,String>();
        private Text word = new Text();

        //Set up cache file from Customer to get the names

        //Map
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String data = value.toString();
            String[] dataLines = data.split("\n");


            for (int i = 1; i < dataLines.length; i++) {


                String[] line = dataLines[i].split("\t");


                String keyDate = line[1];
                word.set(keyDate);


                //String totVal = line[2];


                String valuesSt = keyDate+", 1";//"T~" + 1 + "," + totVal;

                Text values = new Text(valuesSt);


                context.write(word, values);


            }


        }
    }
    public static class TotReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            //String date = "";

            for(Text vals: values){

                String[] valuesArr = vals.toString().split(",");



                sum += Integer.parseInt(valuesArr[1]);


            }
            result = new Text(key+","+sum);



            context.write(key, result);
        }
    }

    public void debug(String[] args) throws Exception {
        long startTime = System.nanoTime();


        //Driver
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CountTweet");
        job.setJarByClass(MapReduceDaily.class);



        job.setMapperClass(Mapper1.class);
        job.setCombinerClass(TotReducer.class);//Combiner
        job.setReducerClass(TotReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(".\\*.tsv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9000/cs585/full_count.csv"));




        System.exit(job.waitForCompletion(true) ? 0 : 1);

        long endTime = System.nanoTime();

        long duration = (endTime - startTime);

        System.out.println(duration);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        long startTime = System.nanoTime();


        //Driver
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CountTweet");
        job.setJarByClass(MapReduceDaily.class);



        job.setMapperClass(Mapper1.class);
        job.setCombinerClass(TotReducer.class);//Combiner
        job.setReducerClass(TotReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(".\\*.tsv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9000/cs585/full_count.csv"));




        System.exit(job.waitForCompletion(true) ? 0 : 1);

        long endTime = System.nanoTime();

        long duration = (endTime - startTime);

        System.out.println(duration);

    }

}


