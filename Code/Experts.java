import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Experts {
	
	public static class Mapper1 extends Mapper<Object,Text,Text,Text>{
		Text person = new Text();
		Text theme = new Text();
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String [] line = value.toString().split("\t");
			String [] persons = line[5].split(";");
			String [] themes = line[3].split(";");
			for(String themeitr : themes){
				for(String personitr : persons){
					theme.set(themeitr);
					person.set(personitr+",1");
					context.write(theme, person);
				}
			}
		}
	}
	
	public static class Reducer1 extends Reducer<Text ,Text, Text,Text>{
		Text outvalue = new Text();
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
			HashMap<String,Integer> hmap = new HashMap<String, Integer>();
			String experts = new String();
			while(values.iterator().hasNext()){
				String [] fields = values.iterator().next().toString().split(",");
				String person = fields[0];
				int count = Integer.parseInt(fields[1]);
				if(hmap.containsKey(person))
					hmap.put(person, hmap.get(person)+count);
				else
					hmap.put(person, count);
			}
			int max = Collections.max(hmap.values());
			if(max>=200){
				for(Entry<String, Integer> entry : hmap.entrySet()){
					if(entry.getValue() == max && !(entry.getKey().isEmpty()))
						experts = experts + entry.getKey()+",";
				}
				if(!experts.isEmpty())
				{
					experts += max;
					outvalue.set(experts);
					context.write(key, outvalue);
				}
			}
		}
	}
	
	public static void main(String ar[]) throws Exception{
		Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(Experts.class);
	    job.setMapperClass(Mapper1.class);
	    job.setReducerClass(Reducer1.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(ar[0]));
	    FileOutputFormat.setOutputPath(job, new Path(ar[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
