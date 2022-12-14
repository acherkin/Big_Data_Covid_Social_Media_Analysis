package org.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CountTweets {

    static String pathC = "C:\\Users\\annac\\OneDrive\\Desktop\\archive\\dailies\\";

    public static void main(String[] args) throws Exception {


        try {
            File myObj = new File("full_count.csv");
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }



        try {
            FileWriter myWriter = new FileWriter("full_count.csv");

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

            int count = 1;


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






                    Path path = Paths.get(pathC+pathD+"\\"+pathD+"-dataset.tsv");

                    long lines = 0;
                    try {

                        // much slower, this task better with sequence access
                        //lines = Files.lines(path).parallel().count();

                        lines = Files.lines(path).count();
                        System.out.println(pathD+" "+lines);
                        myWriter.write(pathD+","+lines+"\n");


                    } catch (IOException e) {
                        e.printStackTrace();
                    }





                }

            }


            myWriter.close();


        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }





// Combining the two workbooks


    }
}
