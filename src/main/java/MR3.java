import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MR3 {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntArrayWritable> {

        private final static IntWritable zero = new IntWritable(0);
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable two = new IntWritable(2);

        private final static IntWritable[] writable01 = {zero,one};
        private final static IntWritable[] writable11 = {one,one};
        private final static IntWritable[] writable12 = {one,two};

        private Text male = new Text("male");
        private Text female = new Text("female");


        public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> output, Reporter reporter) throws IOException {

            String[] items = value.toString().split(";");

            if (items.length >= 2){
                if (items[1].equals("f")){
                    output.collect(female, new IntArrayWritable(writable11));
                    output.collect(male, new IntArrayWritable(writable01));
                }else if (items[1].equals("m")){
                    output.collect(male, new IntArrayWritable(writable11));
                    output.collect(female, new IntArrayWritable(writable01));
                }else {
                    output.collect(male, new IntArrayWritable(writable12));
                    output.collect(female, new IntArrayWritable(writable12));
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, ArrayWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterator<ArrayWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum;
            int sum1 = 0;
            int sum2 = 0;

            while (values.hasNext()) {
                IntWritable val1 = (IntWritable) values.next().get()[0];
                IntWritable val2 = (IntWritable) values.next().get()[1];
                sum1 += val1.get();
                sum2 += val2.get();
            }

            sum = sum1*100/sum2;

            output.collect(key, new FloatWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MR3.class);
        conf.setJobName("Proportion (in%) of male or female");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FloatWritable.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);


        conf.setMapperClass(Map.class);
        //conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}