import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.Path;


import static java.lang.Integer.parseInt;


public class Netflix {
    //Below is the mapper function for counting the number of reviews for each movie
    public static class ReviewsMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 4) {
                int movieID = Integer.parseInt(parts[0]); // taking just movie id
                int userID = Integer.parseInt(parts[1]); // taking just userID
                context.write(new IntWritable(movieID), new IntWritable(userID));
            }
        }
    }
    //Below is the reducer function for counting the number of reviews for each movie
    public static class ReviewsReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;

            for (IntWritable rating : values) {
                total += 1;

            }
            context.write(key, new IntWritable(total));
        }
    }
    //below is the mapper function for getting the average review for a movie with year
    // where key will be movie-YYYY
    public static class AvgRatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context1) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 4) {
                String yearString = parts[3].substring(0, 4); // getting just YYYY from YYYY-MM-DD
                Text yearText = new Text(yearString);
                Text ID = new Text(parts[0] + "-" + yearText.toString());
                double ratings = Double.parseDouble(parts[2]);
                context1.write(new Text(ID), new DoubleWritable(ratings));
            }
        }
    }
    //below is the reudcer function for getting the avg review for a movie with year.
    public static class AvgRatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context1) throws IOException, InterruptedException {
            int total = 0;
            double sum = 0.0;

            for (DoubleWritable rating : values) {
                total += 1;
                sum +=rating.get();

            }
            double avgValue = Math.round((sum /total) * 1e7)/ 1e7; // avg rating value for each movie with YYYY
            context1.write(new Text(key), new DoubleWritable(avgValue));
        }
    }

    /* put your Map-Reduce methods here */

    public static void main ( String[] args ) throws Exception {
        //the below job is to get th reviews for a movie count
        Job job = Job.getInstance();
        job.setJobName("Project1 Reviews");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(Netflix.ReviewsMapper.class);
        job.setReducerClass(Netflix.ReviewsReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);

        // job 2 is for getting the average rating for the movie with Year.
        Job job2 = Job.getInstance();
        job2.setJobName("Project1 Avg Rating");
        job2.setJarByClass(Netflix.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(Netflix.AvgRatingMapper.class);
        job2.setReducerClass(Netflix.AvgRatingReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[0]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        job2.waitForCompletion(true);

    }
}
