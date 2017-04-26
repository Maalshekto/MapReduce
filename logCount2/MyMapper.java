package formation.bigdata.com.logCount2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split(" ");
		
		if (fields.length < 7) {
			return;
		}
		
		String ip = fields[0];
		String url = fields[6];
	
		context.write(new Text(url), new Text(ip));
	}
}