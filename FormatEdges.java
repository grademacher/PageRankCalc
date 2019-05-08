import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FormatEdges{
  public static class EdgeMapper extends Mapper<Object, Text, LongWritable, Text>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
      try{
        context.write(new LongWritable(Long.parseLong(value.toString().split("[\\s\t]+")[0])), new Text(value.toString().split("[\\s\t]+")[1]));
      }catch(Exception e){
        System.println("Bad String: " + value.toString());
        e.printStackTrace();
      }
    }
  }

  public static class EdgeReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
      int total = 0;
      String list = "";
      for(Text i : values){
        total++;
        list += i.toString() + " ";
      }
      context.write(key, new Text(total + ": " + list))
    }
  }

  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Format Edges");
		job.setJarByClass(FormatEdges.class);
		job.setMapperClass(EdgeMapper.class);
		job.setReducerClass(EdgeReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
