package PC1_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import javax.naming.Context;
import java.io.IOException;

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	private static String search;
	public void configure(JobConf job) {
		search = job.get("Key");
	}
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		Text item;
		if(valueString.contains(search)){
			item = new Text(valueString);
		}else{
			item = new Text();
		}
		output.collect(new Text(search), item);
	}
}
