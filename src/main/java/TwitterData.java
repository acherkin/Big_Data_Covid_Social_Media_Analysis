package org.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import static java.lang.System.out;

public class TwitterData {

    String path = "C:\\Users\\annac\\OneDrive\\Desktop\\archive\\dailies\\";


    public static void main(String[] args) {

        out.println("Hello world!");

        TwitterData m = new TwitterData();

        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
            Path file = new Path("hdfs://localhost:9000/cs585/twitter.csv");
            if(hdfs.exists(file)){hdfs.delete(file,true);}

            OutputStream os = hdfs.create(file

            );

            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));

            m.read(br);
            br.close();
            hdfs.close();


        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }


    }
    int[][] dates = {
            {22,31},
            {1,30},
            {1,31},
            {1,30},
            {1,31},
            {1,31},
            {1,30},
            {1,12}
    };



    public void read(BufferedWriter br){
        for(int i = 3; i <= 10; i++){
            String pathM = "";

            if(i < 10){
                pathM="2020-0"+i+"-";
            }else{
                pathM="2020-"+i+"-";
            }

            int ind = i-3;
            for(int j = dates[ind][0]; j<= dates[ind][1]; j++){

                String pathD = "";
                String date = j+"/"+i+"/2020";
                if(j<10){
                    pathD = pathM + "0"+j;
                }else{
                    pathD = pathM +j;
                }



                File fil = new File(path+pathD+"\\"+pathD+"_top1000terms.csv");

                try {
                    Scanner reader = new Scanner(fil);
                    while(reader.hasNextLine()){
                        String data = reader.nextLine()+","+date+"\n";
                        br.write(data);
                    }
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


                File fil2 = new File(path+pathD+"\\"+pathD+"_top1000bigrams.csv");

                try {
                    Scanner reader = new Scanner(fil2);
                    while(reader.hasNextLine()){
                        String data = reader.nextLine()+","+date+"\n";
                        br.write(data);
                    }
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


                File fil3 = new File(path+pathD+"\\"+pathD+"_top1000trigrams.csv");

                try {
                    Scanner reader = new Scanner(fil3);
                    while(reader.hasNextLine()){
                        String data = reader.nextLine()+","+date+"\n";
                        br.write(data);
                    }
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }

        }


    }

}