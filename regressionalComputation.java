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
import org.apache.hadoop.conf.Configured;

public class regressionalComputation extends Configured implements Tool {
	
	public static class sumXTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
	    	//System.out.println(value.toString());
            StringTokenizer itr=new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				y = Double.parseDouble(itr.nextToken());
	        	//System.out.println("x " +  x);
				//System.out.println("y " +  y);
 	        	context.write(new Text("sum"),new DoubleWritable(x));
            } 
        }
    }

    public static class sumXReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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

    public static class sumXSquareTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
            StringTokenizer itr=new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				x = x*x;
				y = Double.parseDouble(itr.nextToken());
		        System.out.println("map"+x);
	 	        context.write(new Text("sum"),new DoubleWritable(x));
            }
            
        }
    }



  	public static class sumXSquareReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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
        }
    }

    public static class sumXCubeTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
    {
        Double x;
		Double y;
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
        {
            StringTokenizer itr=new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                x=Double.parseDouble(itr.nextToken());
				x = x*x*x;
				y = Double.parseDouble(itr.nextToken());
		        System.out.println("map"+x);
	 	        context.write(new Text("sum"),new DoubleWritable(x));
            }
        }
    }

  	public static class sumXCubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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
        }
    }

    public static class xToFourTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
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
				x = x*x*x*x;
				y = Double.parseDouble(itr.nextToken());
		        System.out.println("map"+x);
	 	        context.write(new Text("sum"),new DoubleWritable(x));
            }
        }
    }



  	public static class xToFourReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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

	public static class sumYTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
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
	 	        context.write(new Text("sum"),new DoubleWritable(y));
            }
        }
    }



  	public static class sumYReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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

    public static class sumXYTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
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
	 	        context.write(new Text("sum"),new DoubleWritable(x*y));
            }
        }
    }



  	public static class sumXYReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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

    public static class sumXSquareYTokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> 
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



  	public static class sumXSquareYReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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


    public static void main(String[] args) throws Exception 
    {
    	String myArgs[];
    	try 
    	{
            InputStream fis = new FileInputStream("sample.txt");
            InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
            BufferedReader br = new BufferedReader(isr);
            String line;
            line = br.readLine();
            int jobSize = Integer.parseInt(line);
            
            myArgs = new String[jobSize+2];
            myArgs[0] = line;
            myArgs[1] = "2";
            int i = 2;
            while ((line = br.readLine()) != null) {
                myArgs[i++] = line;
                //System.out.println(line);
            }
        } 
        catch (Exception e) 
        {
            System.out.println(Arrays.toString(e.getStackTrace()));
        } 

        int size = Integer.parseInt(myArgs[0]);

        for (int i = 0; i < size; i++) 
        {
        	int res = ToolRunner.run(new Configuration(), new ToolMapReduce(), myArgs);
            //t.print(myArgs);
            int iter = Integer.parseInt(myArgs[1]);
            iter++;
            myArgs[1] = String.valueOf(iter);
            //System.out.println("");
        }

		//int res = ToolRunner.run(new Configuration(), new ToolMapReduce(), args);
		//System.exit(res);

	@Override

	public int run(String args[]) throws Exception
	{

		int iter = Integer.parseInt(args[1]);
        if (iter == 0) {
        	Configuration conf = this.getConf();
		
			Job job=Job.getInstance(conf, "sumX");
			job.setJarByClass(ToolMapReduce.class);    
			
			job.setMapperClass(sumXTokenizerMapper.class);
			job.setReducerClass(sumXReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			 
			FileInputFormat.addInputPath(job, new Path("Input"));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.submit();
			

			Configuration conf1 = this.getConf();
			
			Job job1=Job.getInstance(conf, "number sumsq");
			job1.setJarByClass(ToolMapReduce.class);    
			
			job1.setMapperClass(TokenizerMapper1.class);
			job1.setReducerClass(IntSumReducer1.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(DoubleWritable.class);
			 
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));

			job1.submit();

			int a = (job.waitForCompletion(true)?0:1);
			int b = (job1.waitForCompletion(true)?0:1);

			return a+b;
        }

        else if (iter == 1) {
        	
        }

        else if (iter == 2) {
        	
        }

        else if (iter == 3) {
        	
        }

        else if (iter == 4) {
        	
        }

        else if (iter == 5) {
        	
        }

        else if (iter == 6) {
        	
        }



        System.out.println(args[iter]+"TokenizerMapper");
        System.out.println(args[iter]+"Reducer");
        System.out.println("Output"+args[iter]);
	}

	/*public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		
		Job job=Job.getInstance(conf, "sum");
		job.setJarByClass(ToolMapReduce.class);    
		
		job.setMapperClass(args[iter].class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.submit();
		

		Configuration conf1 = this.getConf();
		
		Job job1=Job.getInstance(conf, "number sumsq");
		job1.setJarByClass(ToolMapReduce.class);    
		
		job1.setMapperClass(TokenizerMapper1.class);
		job1.setReducerClass(IntSumReducer1.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		 
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		job1.submit();

		int a = (job.waitForCompletion(true)?0:1);
		int b = (job1.waitForCompletion(true)?0:1);

		return a+b;
	}*/



}