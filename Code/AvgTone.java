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

public class AvgTone {
	
	public static class Mapper1 extends Mapper<Object,Text,IntWritable,Text>{
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String [] line = value.toString().split("\t");
			context.write(new IntWritable(Integer.parseInt(line[0].toString())), new Text(line[7]+",1")); //count added
		}
	}

	public static class Reducer1 extends Reducer<IntWritable, Text, IntWritable, Text>{
		String outValue = new String();
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			float sumData[] = new float[7];
			for(Text data : values){
				String [] toneData = data.toString().split(",");
				for(int i=0; i<7; i++){
					sumData[i]+=Float.parseFloat(toneData[i]);
				}
			}
			for(int i=0; i<6; i++)
				sumData[i] = sumData[i]/sumData[6];
			for(int i=0; i<6; i++)
				outValue = outValue + sumData[i] + ",";
			outValue += sumData[6];
			context.write(key, new Text(outValue));
		}
	}
	
	public static void main(String ar[]) throws Exception{
		Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(AvgTone.class);
	    job.setMapperClass(Mapper1.class);
	    job.setReducerClass(Reducer1.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(ar[0]));
	    FileOutputFormat.setOutputPath(job, new Path(ar[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
