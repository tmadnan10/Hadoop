import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured

public class ToolMapReduce extends Configured implements Tool {

	public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
	Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
	    System.out.println(value.toString());
            StringTokenizer itr=new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
		y = Double.parseDouble(itr.nextToken());
	        System.out.println("x " +  x);
		System.out.println("y " +  y);
 	        context.write(new Text("sum"),new DoubleWritable(x));
            }
            
        }
    }



  public static class IntSumReducer
       extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException
        {
            Double sum=0.0;
	    Double n = 0.0;
            for(DoubleWritable value:values)
                {
                    Double temp=value.get();
		    n++;
		    sum +=temp;
		    System.out.println("reduce"+sum);
                }
            context.write(key,new DoubleWritable(sum));
	    context.write(new Text("n"),new DoubleWritable(n));
        }
    }


	public static void main(String[] args) {
		int res = ToolRunner.run(new Configuration(), new ToolMapReduce(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		
		Job job=Job.getInstance(conf, "number sum");
		job.setJarByClass(ToolMapReduce.class);    
        
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
         
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
	}
	
}