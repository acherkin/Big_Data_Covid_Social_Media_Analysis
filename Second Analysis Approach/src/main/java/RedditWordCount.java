import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;


public class RedditWordCount {


    public static class DateMapper extends Mapper<Object, Text, Text, Text> {

        private Text date = new Text();
        private final Date beginTime = epochToDate("1584835200");
        private final Date endTime = epochToDate("1602547199");
        Calendar calendar = Calendar.getInstance();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //filter dates to match twitter dataset
            // -> from [3/22/2020 to 10/12/2020]
            //-> from [1584835200 -> 1602547199]

            String valueString = value.toString();
            String[] singlePostData = valueString.split(",");
            //transform epoch string to date
            Date dateToCheck = epochToDate(singlePostData[5]);
            //check if date is in specified range
            if(dateToCheck.compareTo(beginTime) > 0) {
                if(dateToCheck.compareTo(endTime) < 0){
                    //filter down to day month year for use as key for joining titles
                    calendar.setTime(dateToCheck);
                    int day = calendar.get(Calendar.DAY_OF_MONTH);
                    int month = calendar.get(Calendar.MONTH) + 1;
                    int year = calendar.get(Calendar.YEAR);

                    date.set(day + "-" + month + "-" + year);
                    Text title = new Text(singlePostData[10]);

                    context.write(date, title);
                }
            }


        }
    }


    public static class DateReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder allWords = new StringBuilder();

            for (Text t : values) {
                String[] words = t.toString().split("([^A-Za-z0-9]+)"); //splits if not alphanumeric string is given
                for(String word : words){
                    //TODO: filter out connecting words
                    allWords.append(word).append("-");
                }
            }
            context.write(key, new Text(allWords.toString()));
        }
    }

    public static class DateWordMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text dateWordKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] dateWithWords = valueString.split(",");
            String date = dateWithWords[0];
            String[] words = dateWithWords[1].split("-");
            for(String word : words){
                dateWordKey.set(date +  "-" + word);
                context.write(dateWordKey, one);
            }
        }
    }

    public static class DateWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static Date epochToDate(String date){
        return new Date(Long.parseLong(date) * 1000);
    }


    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Map2Date");
        job1.setJarByClass(RedditWordCount.class);

        job1.setMapperClass(DateMapper.class);
        job1.setReducerClass(DateReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // MapReduce Job 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "DateSpecificWordCount");
        job2.setJarByClass(RedditWordCount.class);

        job2.setMapperClass(DateWordMapper.class);
        job2.setReducerClass(DateWordReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }


    public static void main(String[] args) throws Exception {

    }
}
