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

public class ActivePeople {
	
	public static class Mapper1 extends Mapper<Object,Text,Text,IntWritable>{
		private final static IntWritable num = new IntWritable(1) ;
		Text person = new Text();
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String [] line = value.toString().split("\t");
			StringTokenizer persons = new StringTokenizer(line[5], ";");
			while(persons.hasMoreTokens()){
				person.set(persons.nextToken());
				context.write(person,num);
			}
		}
	}
	
	public static class Reducer1 extends Reducer<Text ,IntWritable, Text,IntWritable>{
		private static IntWritable result  = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values, Context context)
				throws IOException,InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+= val.get();
			}
			if(sum >100)
			{
				result.set(sum);
				context.write(key, result);
			}
		}
		
	}
	
	public static void main(String ar[]) throws Exception{
		Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(ActivePeople.class);
	    job.setMapperClass(Mapper1.class);
	    job.setCombinerClass(Reducer1.class);
	    job.setReducerClass(Reducer1.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(ar[0]));
	    FileOutputFormat.setOutputPath(job, new Path(ar[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
