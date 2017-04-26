package formation.bigdata.com.logCount1;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text resource, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
	{
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		con.write(resource, new IntWritable(sum));
	}
}
