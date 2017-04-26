package formation.bigdata.com.logCount1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split(" ");
		
		if(fields.length < 3) {
			return;
		}
		
		String resource = fields[2];
		
		if (!resource.toLowerCase().startsWith("http")) {
			return;
		}
	
		context.write(new Text(resource), new IntWritable(1));
		
	}
}
