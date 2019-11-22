import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountryTone {
	
	public static class Mapper1 extends Mapper<Object,Text,DoubleWritable,IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		DoubleWritable tone = new DoubleWritable();
		static boolean hasCountry = false;
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String [] line = value.toString().split("\t");
			String [] locations = line[4].split(";");
			for(String place : locations){
				String[] country = place.split("#");
				if(country[2].equals("IN"))
					hasCountry=true;
			}
			if(hasCountry){
				String [] toneNums = line[7].split(",");
				double num = Double.parseDouble(toneNums[0]);
				BigDecimal bd = new BigDecimal(num);
				bd = bd.setScale(2, RoundingMode.HALF_UP);
				tone.set(bd.doubleValue());
				context.write(tone, one);	
			}
			hasCountry=false;
		}
	}
	
	public static class Reducer1 extends Reducer<DoubleWritable ,IntWritable, DoubleWritable,IntWritable>{
		private static IntWritable result  = new IntWritable();
		public void reduce(DoubleWritable key,Iterable<IntWritable> values, Context context)
				throws IOException,InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+= val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	public static void main(String ar[]) throws Exception{
		Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(CountryTone.class);
	    job.setMapperClass(Mapper1.class);
	    job.setCombinerClass(Reducer1.class);
	    job.setReducerClass(Reducer1.class);
	    job.setOutputKeyClass(DoubleWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(ar[0]));
	    FileOutputFormat.setOutputPath(job, new Path(ar[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
