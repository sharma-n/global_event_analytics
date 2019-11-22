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

public class PeopleNetwork {
	
	public static class Mapper1 extends Mapper<Object,Text,Edge,IntWritable>{
		private static IntWritable one = new IntWritable(1);
		private Text firstNode = new Text();
		private Text secondNode = new Text();
		private Edge edge = new Edge();
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String [] line = value.toString().split("\t");
			String [] people = line[5].split(";");
			for (int i=0; i<people.length-1; i++){
				for (int j=i+1; j<people.length; j++){
					if(people[i].compareTo(people[j])<0){
						firstNode.set(people[i]);
						secondNode.set(people[j]);
					} else {
						firstNode.set(people[j]);
						secondNode.set(people[i]);
					}
					edge.set(firstNode, secondNode);
					context.write(edge, one);
				}
			}
		}
	}
	
	public static class Combiner1 extends Reducer<Edge ,IntWritable, Edge,IntWritable>{
		private static IntWritable result  = new IntWritable();
		public void reduce(Edge key,Iterable<IntWritable> values, Context context)
				throws IOException,InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+= val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class Reducer1 extends Reducer<Edge ,IntWritable, Edge,IntWritable>{
		private static IntWritable result  = new IntWritable();
		public void reduce(Edge key,Iterable<IntWritable> values, Context context)
				throws IOException,InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+= val.get();
			}
			if(sum>5){
				result.set((int)(1.0/sum*1000));
				context.write(key, result);
			}
		}
	}
	
	public static void main(String ar[]) throws Exception{
		Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(PeopleNetwork.class);
	    job.setMapperClass(Mapper1.class);
	    job.setCombinerClass(Combiner1.class);
	    job.setReducerClass(Reducer1.class);
	    job.setOutputKeyClass(Edge.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(ar[0]));
	    FileOutputFormat.setOutputPath(job, new Path(ar[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}

