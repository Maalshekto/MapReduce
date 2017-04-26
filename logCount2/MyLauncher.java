package formation.bigdata.com.logCount2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import formation.bigdata.com.logCount2.MyMapper;
import formation.bigdata.com.logCount2.MyReducer;

public class MyLauncher {

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	    Job job = Job.getInstance(conf, "Log_Analyse");
	    job.setJarByClass(MyMapper.class);
	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
//	    FileInputFormat.addInputPath(job, new Path("/user/cloudera/mapreduce/wordCount"));
//	    FileOutputFormat.setOutputPath(job, new Path("/user/cloudera/wordCount/output"));
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}