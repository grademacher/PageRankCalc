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


public class PageRank{
  public static class RankMapper extends Mapper<Object, Text, LongWritable, Text>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
      String[] temp = value.toString().split("[\\s\t]+");
      String data = "data " + temp[1] + " " + temp[2] + " " + temp[3] + " ";
      for(int i = 4; i < temp.length; i++){
        data = data + temp[i] + " ";
        context.write(new LongWritable(Long.parseLong(temp[i])), new Text(Double.toString(((double) Double.parseDouble(temp[2]) / Double.parseDouble(temp[1])))));
      }
      context.write(new LongWritable(Long.parseLong(temp[0])), new Text(data));
    }
  }

  public static class RankReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
      String edges = "";
      String r_new = "";
      String nodes = "";
      double increment = 0;

      for(Text info : values){
        String[] temp = info.toString().split("[\\s\t]+");
        if(temp[0].equals("data")){
          edges = temp[1];
          r_new = temp[3];
          for(int i = 4; i < temp.length; i++){ nodes += temp[i] + " "; }
        }else{
          increment += Double.parseDouble(temp[0]);
        }
      }

      context.write(key, new Text(edges + " " + increment + " " + increment + " " + nodes));
    }
  }

  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Calculate PageRank");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(RankMapper.class);
		job.setReducerClass(RankReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
