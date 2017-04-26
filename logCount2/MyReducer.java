package formation.bigdata.com.logCount2;
import java.io.IOException;


import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text word, Iterable<Text> values, Context con) throws IOException, InterruptedException
	{
		Set<Text> uniqueIp = new HashSet<Text>();
		for (Text val : values) {
			uniqueIp.add(val);
		}	
		con.write(word, new IntWritable(uniqueIp.size()));
	}
}
