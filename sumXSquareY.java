import java.io.*;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class sumXSquareY {

  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
	Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
	 
            StringTokenizer itr=new StringTokenizer(value.toString());
	    //n = a = itr.nextToken();
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
		y = Double.parseDouble(itr.nextToken());
		
	        System.out.println("map"+x);
 	        context.write(new Text("sum"),new DoubleWritable(x*x*y));
            }
            
        }
    }



  public static class IntSumReducer
       extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
        {
            Double sum=0.0;
            for(DoubleWritable value:values)
                {
                    Double temp=value.get();
		    sum +=temp;
		    System.out.println("reduce"+sum);
                }
            context.write(key,new DoubleWritable(sum));
	    //context.write(new Text("n"),new DoubleWritable(5.0));
        }
    }


  public static void main(String[] args) throws Exception {
// TODO Auto-generated method stub
            Configuration conf=new Configuration();
            String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
            if(otherArgs.length!=2)
            {
                System.err.println("Error");
                System.exit(2);
            }
            Job job=Job.getInstance(conf, "number sum");
            job.setJarByClass(sumXSquareY.class);
            job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true)?0:1);

  }
}

