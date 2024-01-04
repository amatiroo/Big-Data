import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.Path;

class DataOne implements Writable {
    public int movieID;
    public int userID;
    public int rating;
    public String year;

    DataOne() {}

    DataOne(int n, int d, int a, String z) {
        movieID = n; userID = d; rating = a; year = z;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(movieID);
        out.writeInt(userID);
        out.writeInt(rating);
        out.writeUTF(year);
    }

    public void readFields(DataInput in) throws IOException {
        movieID = in.readInt();
        userID = in.readInt();
        rating = in.readInt();
        year = in.readUTF();
    }
}

class DataTwo implements Writable {
    public int movieID;
    public String year;
    public String title;

    DataTwo() {}

    DataTwo(int a, String n, String d) {
        movieID = a; year = n; title = d;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(movieID);
        out.writeUTF(year);
        out.writeUTF(title);
    }

    public void readFields(DataInput in) throws IOException {
        movieID = in.readInt();
        year = in.readUTF();
        title = in.readUTF();
    }
}

class Result implements Writable {

    public String year;
    public String movieTitle;

    Result(String yr, String mt) {
        year = yr; movieTitle = mt;
    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(year);
        out.writeUTF(movieTitle);
    }

    public void readFields(DataInput in) throws IOException {

        year = in.readUTF();
        movieTitle = in.readUTF();
    }

    public String toString() {
        return year + ": " + movieTitle;
    }
}

class DataOneTwo implements Writable {
    public short tag;
    public DataOne data1;
    public DataTwo data2;

    DataOneTwo() {}

    DataOneTwo(DataOne e) {
        tag = 0; data1 = e;
    }

    DataOneTwo(DataTwo d) {
        tag = 1; data2 = d;
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(tag);
        if (tag == 0)
            data1.write(out);
        else
            data2.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readShort();
        if (tag == 0) {
            data1 = new DataOne();
            data1.readFields(in);
        } else {
            data2 = new DataTwo();
            data2.readFields(in);
        }
    }
}

public class Netflix {
    public static class DataOneMapper extends Mapper<Object,Text,IntWritable,DataOneTwo > {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            DataOne e = new DataOne(s.nextInt(),s.nextInt(),s.nextInt(),s.next());
            context.write(new IntWritable(e.movieID),new DataOneTwo(e));
            s.close();
        }
    }

    public static class DataTwoMapper extends Mapper<Object,Text,IntWritable,DataOneTwo > {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            DataTwo d = new DataTwo(s.nextInt(),s.next(),s.next());
            context.write(new IntWritable(d.movieID),new DataOneTwo(d));
            s.close();
        }
    }

    public static class ResultReducer extends Reducer<IntWritable,DataOneTwo,Result,DoubleWritable> {
        static Vector<DataOne> d1 = new Vector<DataOne>();
        static Vector<DataTwo> d2 = new Vector<DataTwo>();
        @Override
        public void reduce ( IntWritable key, Iterable<DataOneTwo> values, Context context )
                throws IOException, InterruptedException {
            d1.clear();
            d2.clear();
            for (DataOneTwo v : values)
                if (v.tag == 0)
                    d1.add(v.data1);
                else d2.add(v.data2);

            double sum = 0.0;
            int count = 0;

            for (DataOne s : d1) {
                sum += s.rating;
                count++;

            }
            double avgrating = 0;
            DataTwo d = d2.get(0);
            if (count != 0) {
                avgrating = Math.round((sum /count) * 1e7)/ 1e7;
                context.write(new Result(d.year, d.title), new DoubleWritable(avgrating));

            }


        }
    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJobName("Netflix rating project2");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Result.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DataOneTwo.class);
        job.setReducerClass(ResultReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DataOneMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DataTwoMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
