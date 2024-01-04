
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



class Tagged implements Writable {
    public boolean tag;
    public int distance;
    public Vector<Integer> following;

    Tagged() {
        tag = false;
    }

    Tagged(int d) {
        tag = false;
        distance = d;
    }

    Tagged(int d, Vector<Integer> f) {
        tag = true;
        distance = d;
        following = f;
    }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(tag);
        out.writeInt(distance);
        if (tag) {
            out.writeInt(following.size());
            for (int i = 0; i < following.size(); i++)
                out.writeInt(following.get(i));
        }
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        if (tag) {
            int n = in.readInt();
            following = new Vector<Integer>(n);
            for (int i = 0; i < n; i++)
                following.add(in.readInt());
        }
    }

}

public class Graph {
    static int start_id = 14701391;
    static int max_int = Integer.MAX_VALUE;

    public static class ReadGraphMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int follower;
            int id ;
            id = s.nextInt();
            follower = s.nextInt();

            context.write(new IntWritable(follower),new IntWritable(id));
            s.close();

        }
    }

    public static class ReadGraphReducer extends Reducer<IntWritable, IntWritable, IntWritable, Tagged> {
        static Vector<Integer> data = new Vector<>();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            data.clear();
            for (IntWritable v : values) {
                data.add(v.get());
            }
            if (key.get() == start_id || key.get() == 1) {
                context.write(key, new Tagged(0, data));
            } else {
                context.write(key, new Tagged(max_int, data));
            }
        }
    }


    public static class DistanceMapper extends Mapper<IntWritable, Tagged, IntWritable, Tagged> {

        @Override
        public void map(IntWritable key, Tagged v, Context context1)
                throws IOException, InterruptedException {

            context1.write(key,v);

            if (v.distance < max_int) {
                for (Integer id : v.following) {
                    int myDIstance = v.distance+1;
                    context1.write(new IntWritable(id) , new Tagged(myDIstance));
                }
            }


        }
    }

    public static class DistanceReducer extends Reducer<IntWritable, Tagged, IntWritable, Tagged> {

        @Override
        public void reduce(IntWritable key, Iterable<Tagged> values, Context context)
                throws IOException, InterruptedException {

            int m = max_int;
            Vector<Integer> follow =  new Vector<>();
            for (Tagged v : values) {
                if (v.distance < m) {
                    m = v.distance;
                }
                if (v.tag) {
                    follow.addAll(v.following);
                }
            }

            context.write(key, new Tagged(m, follow));
        }
    }

    public static class FinalMapper extends Mapper<IntWritable, Tagged, IntWritable, IntWritable> {
        @Override
        public void map(IntWritable key, Tagged value, Context context)
                throws IOException, InterruptedException {
            if (value.distance < max_int) {
                context.write(key, new IntWritable(value.distance));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int iterations = 5;
        Configuration conf = new Configuration();

        // First Map-Reduce job to read the graph
        Job job = Job.getInstance(conf, "Mapper1");
        job.setJarByClass(Graph.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Tagged.class);
        job.setReducerClass(ReadGraphReducer.class);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, ReadGraphMapper.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "0"));
        job.waitForCompletion(true);

        for (short i = 0; i < iterations; i++) {
            Job job1 = Job.getInstance(conf, "Mapper2");
            job1.setJarByClass(Graph.class);
            job1.setMapOutputKeyClass(IntWritable.class);
            job1.setMapOutputValueClass(Tagged.class);
            job1.setOutputKeyClass(IntWritable.class);
            job1.setOutputValueClass(Tagged.class);
            job1.setReducerClass(DistanceReducer.class);
            MultipleInputs.addInputPath(job1,new Path(args[1]+i), SequenceFileInputFormat.class, DistanceMapper.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileOutputFormat.setOutputPath(job1, new Path(args[1] + (i + 1)));
            job1.waitForCompletion(true);
        }

        Job job3 = Job.getInstance(conf, "Mapper3");
        job3.setJarByClass(Graph.class);
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job3,new Path(args[1] + iterations), SequenceFileInputFormat.class, FinalMapper.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}