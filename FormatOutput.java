import java.io.*;
import java.util.*;
import java.lang.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FormatOutput{
  public static class FormatMapper extends Mapper<Object, Text, DoubleWritable, Text>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
      String[] temp = value.toString().split("[\\s\t]+");
      context.write(new DoubleWritable(Double.parseDouble(temp[3])*-1), new Text(temp[0]));
    }
  }

  public static class FormatReducer extends Reducer<DoubleWritable, Text, Text, Text>{
    private boolean file_read = false;
    private HashMap<String, String> hashmap = new HashMap<String, String>();

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
      if(!file_read){
        try{
          //URL file_url = new URL("https://s3.amazonaws.com/417pagerank/wiki-topcats-page-names.txt"); //not working
          URL file_url = new URL("https://s3.amazonaws.com/417pagerankproject/extrainput/wiki-topcats-page-names.txt"); //working
          BufferedReader file_reader = new BufferedReader(new InputStreamReader(file_url.openStream()));
          String line;
          while((line = file_reader.readLine()) != null){
            String[] temp = line.split("[\\s\t]+");

            String node_num = temp[0];
            String node_name = "";
            for(int i = 1; i < temp.length; i++){
              node_name = node_name + temp[i] + " ";
            }
            hashmap.put(node_num, node_name);
          }
        }catch(Exception e){
          e.printStackTrace();
        }
        file_read = true;
      }

      //get the name of the node out of the hashmap from the value that was passed with the
      for(Text value : values){
        context.write(new Text(value.toString() + " \t" + hashmap.get(value.toString())), new Text(Double.toString((key.get() * -1))));
      }
    }
  }

  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Format PageRank Output");
		job.setJarByClass(FormatOutput.class);
		job.setMapperClass(FormatMapper.class);
		job.setReducerClass(FormatReducer.class);
    job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
