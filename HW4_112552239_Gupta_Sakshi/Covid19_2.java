package Covid19_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

//import java.text.ParseException;
//import java.text.SimpleDateFormat;  
//import java.util.Date;  
public class Covid19_2 extends Configured implements Tool{
	
	class DateException extends Exception 
	{ 
	    public DateException(String s) 
	    {
	        System.out.println(s); 
	        System.exit(1);
	    } 
	} 
	public class Covid{
		public static final int date = 0;
		public static final int country = 1;
		public static final int new_cases = 2;
		public static final int new_deaths = 3;
	}

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        Text out_country = new Text();
        IntWritable out_new_deaths = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//System.out.print("This is a random print");
        	String[] input = value.toString().split(",");
        	Configuration co = context.getConfiguration();
        	String st_date=co.get("start_date");
        	String en_date=co.get("end_date");
        	
        	//SimpleDateFormat formatter1=new SimpleDateFormat("YYYY-MM-DD");  
			//Date date1=formatter1.parse(st_date);
			//Date date2=formatter1.parse(en_date);  
			 
        	if(input[3].equalsIgnoreCase("new_deaths")) return;
        	if(input[Covid.date].compareTo(st_date)>=0 && input[Covid.date].compareTo(en_date)<=0)
        	{
        		out_country.set(input[Covid.country]);
        		out_new_deaths.set(Integer.parseInt(input[Covid.new_deaths]));
        		context.write(out_country, out_new_deaths);
        	}
        	}
        	
        }
 
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable output = new IntWritable();

        public void reduce(Text country, Iterable<IntWritable> new_deaths,Context context) throws IOException, InterruptedException {
            int deaths = 0;
            for (IntWritable val : new_deaths) {
                deaths += val.get();
            }
            output.set(deaths);
            context.write(country, output);
        }
    }

    	public int run(String[] args) throws Exception {
    	//System.out.print("|" + args[1] + "|");
    	//System.out.print("|true|");
        Configuration conf = new Configuration();  
        conf.set("start_date", args[1]);
        conf.set("end_date", args[2]);
        try {
        if(!(args[1].compareTo("2019-12-31")>=0 && args[2].compareTo("2020-04-08")<=0 && args[1].compareTo(args[2])<=0 )) {
        	throw new DateException("Invalid Date Range");    		
        }
        }
        catch(Exception e)
        {
        		//e.printStackTrace();
        }
    	//Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Covid19_2");
        job.setJarByClass(Covid19_2.class);
        job.setMapperClass(TokenizerMapper.class); 
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
    	long startTime = System.currentTimeMillis();
        int res = ToolRunner.run(new Configuration(), new Covid19_2(), args);
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("Execution time for Task 2(Hadoop)= "+elapsedTime+" milliseconds");
        System.exit(res);
    }
}