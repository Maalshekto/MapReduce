package formation.bigdata.com.reservation;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import formation.bigdata.com.reservation.MyMapper;
import formation.bigdata.com.reservation.MyReducer;

public class MyLauncher {
	
	/* Generic set output separator function */
	public static void setTextOutputFormatSeparator(final Job job, final String separator){
		final Configuration conf = job.getConfiguration(); //ensure accurate config ref

		conf.set("mapred.textoutputformat.separator", separator); //Prior to Hadoop 2 (YARN)
		conf.set("mapreduce.textoutputformat.separator", separator);  //Hadoop v2+ (YARN)
		conf.set("mapreduce.output.textoutputformat.separator", separator);
		conf.set("mapreduce.output.key.field.separator", separator);
		conf.set("mapred.textoutputformat.separatorText", separator); // ?
	}

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
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    MyLauncher.setTextOutputFormatSeparator(job, ",");
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
