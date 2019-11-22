import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DeathToll {
	
	public static class Mapper1 extends Mapper<Object,Text,Text,IntWritable>{
		IntWritable toll = new IntWritable();
		Text location = new Text();
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			int num = 0;
			String state = new String();
			String [] line = value.toString().split("\t");
			if(!line[2].isEmpty()){
				String [] counts = line[2].split(";");
				for(String count : counts){
					String [] type = count.split("#");
					if(type[0].equals("KILL") && (type[3].equals("3") || type[3].equals("4"))  && type[1].length()<5){
						num = Integer.parseInt(type[1]);
						state = type[4].toLowerCase();
						location.set(state);
						toll.set(num);
						context.write(location, toll);
					}
				}
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
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	public static void main(String ar[]) throws Exception{
		Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(DeathToll.class);
	    job.setMapperClass(Mapper1.class);
	    job.setReducerClass(Reducer1.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(ar[0]));
	    FileOutputFormat.setOutputPath(job, new Path(ar[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
