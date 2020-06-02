package Covid19_1;

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

public class Covid19_1 extends Configured implements Tool{
	
	public class Covid{
		public static final int date=0;
		public static final int country=1;
		public static final int new_cases=2;
		public static final int new_deaths=3;
	}

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        Text out_country = new Text();
        IntWritable out_new_cases = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//System.out.print("This is a random print");
        	String[] input=value.toString().split(",");
        	Configuration co=context.getConfiguration();
        	String flag1=co.get("flag");
        	if(input[2].equalsIgnoreCase("new_cases")) return;
        	
        	if(flag1.equals("true")) {
        	if(!input[Covid.date].equals("2019-12-31"))
        	{
        		out_country.set(input[Covid.country]);
        		out_new_cases.set(Integer.parseInt(input[Covid.new_cases]));
        		context.write(out_country, out_new_cases);
        	}
        	}
        	else {
        	if(!input[Covid.date].equals("2019-12-31"))
            {
        			if(input[Covid.country].equals("World")) return;
            		out_country.set(input[Covid.country]);
            		out_new_cases.set(Integer.parseInt(input[Covid.new_cases]));
            		context.write(out_country, out_new_cases);
            }
        	}
        }
    }
 
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable output = new IntWritable();

        public void reduce(Text country, Iterable<IntWritable> new_cases, Context context) throws IOException, InterruptedException {
            int cases = 0;
            for (IntWritable val : new_cases) {
            	cases += val.get();
            }
            output.set(cases);
            context.write(country, output);
        }
    }

    	public int run(String[] args) throws Exception {
    	//System.out.print("|" + args[1] + "|");
    	//System.out.print("|true|");
        Configuration conf = new Configuration();
        conf.set("flag", args[1]);
    	//Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Covidid19_1");
        job.setJarByClass(Covid19_1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Covid19_1(), args);
        System.exit(res);
    }
}
