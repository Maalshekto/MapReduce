package formation.bigdata.com.reservation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		public final int DATE_FIELD_IND =  0;
		public final int INOUT_FIELD_IND =  1;
		public final int AUSTRACITY_FIELD_IND = 2;
		public final int STOP_FIELD_IND =  10;		 
		public final int MAXSEAT_FIELD_IND =  12;
		
		public final int MINIMUM_FIELDS = 13;
		
		public final String FIELDS_SEPARATOR = ";";
		public final String DATE_SEPARATOR = "-";	
		
		public final int DATE_MONTH_ID = 0;
		public final int DATE_YEAR_ID = 1;
		
		public final String INOUT_VALUE_OUT = "O"; 
		public final int NO_STOP = 0;
		public final String REQUESTED_CITY = "Sydney";
		
	
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		/*
		 * Retrieving fields lists and check 
		 * that there is a minimum number of fields. 
		 */
			
		String line = value.toString();
		String[] fields = line.split(FIELDS_SEPARATOR);
		
		if (fields.length < MINIMUM_FIELDS) {
			return;
		}

		/*
		 * Data extraction of the observation. Drop the observation by testing
		 * one by one if a field has an unexpected value.		 
		 */
		
		/* City */
		String city = fields[AUSTRACITY_FIELD_IND];
		if (!city.equals(REQUESTED_CITY)) {
			return;
		}
		
		/* InputOutput */
		String inOut = fields[INOUT_FIELD_IND];
		if(!inOut.equals(INOUT_VALUE_OUT)) {
			return;
		}
		
		/* Stop value */
		int stop = Integer.parseInt(fields[STOP_FIELD_IND]);
		if( stop != NO_STOP) {
			return;
		}
		
		/* Reformat Date in order to have a well sorted results from reducer. */
		String date = fields[DATE_FIELD_IND];
		String[] dateFields = date.split(DATE_SEPARATOR);
		date = dateFields[DATE_YEAR_ID] + "-" + dateFields[DATE_MONTH_ID];
		
		
		int maxSeats = Integer.parseInt(fields[MAXSEAT_FIELD_IND]);
	
		context.write(new Text(date), new IntWritable(maxSeats));
		
	}
}