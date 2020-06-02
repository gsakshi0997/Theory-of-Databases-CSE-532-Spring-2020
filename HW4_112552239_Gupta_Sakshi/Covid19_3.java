package Covid19_3;

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

public class Covid19_3 extends Configured implements Tool{

    public class Covid{
        public static final int date = 0;
        public static final int country = 1;
        public static final int new_cases = 2;
        public static final int new_deaths = 3;

    }
    private static HashMap<String, Long> pop_map = new HashMap<String, Long>();
    
    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        Text out_country = new Text();
        DoubleWritable out_new_cases = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] input = value.toString().split(",");
            if(input[2].equals("new_cases"))  return;
            if(!input[Covid.date].equals("2019-12-31"))
            {
            	if(input[Covid.country].equals("World") || input[Covid.country].equals("International")) return;
                out_country.set(input[Covid.country]);
                out_new_cases.set(Double.parseDouble(input[Covid.new_cases]));
                context.write(out_country, out_new_cases);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable output = new DoubleWritable();
        private BufferedReader bufferedReader;
        @Override
        public void setup(Context context) throws IOException{

            URI[] files = context.getCacheFiles();
            String str = "";
            for (URI file : files){

                Path f = new Path(file);
                bufferedReader = new BufferedReader(new FileReader(f.getName()));

                while ((str = bufferedReader.readLine()) != null) {
                    String[] pop_array = str.split(",");
                    
                    if(pop_array.length==5 && !pop_array[4].isEmpty())
                    {
                        if(pop_array[4].equals("population")) continue;  
                        pop_map.put(pop_array[1],Long.parseLong(pop_array[4]));
                    }
                }
                //System.out.println(PopulationMap.size());
            }
        }

        public void reduce(Text country, Iterable<DoubleWritable> new_cases,Context context) throws IOException, InterruptedException {
            double cases = 0;
            for (DoubleWritable val : new_cases) {
            	cases += val.get();

            }
            //if(!country.toString().equals("World") || !country.toString().equals("International")) {
            if(pop_map.containsKey(country.toString()))
            {
                double res=0.0;
                res = (cases/(double)(pop_map.get(country.toString())))*1000000;
                //System.out.println(country.toString());
                //System.out.println(res);
                output.set(res);
                context.write(country,output);
            } 
        }
    }

    public int run(String[] args) throws Exception {
    	//System.out.print("|" + args[1] + "|");
    	//System.out.print("|true|");
    	//Configuration conf = this.getConf();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Covid19_3");
        job.addCacheFile(new Path(args[1]).toUri());
        job.setJarByClass(Covid19_3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
    	long startTime = System.currentTimeMillis();
    	int res = ToolRunner.run(new Configuration(), new Covid19_3(), args);
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("Execution time for Task 3(Hadoop)= "+elapsedTime+" milliseconds");
        System.exit(res);
    }
   
}