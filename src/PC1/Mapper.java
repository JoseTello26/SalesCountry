package PC1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, DoubleWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] rowData = valueString.split(",");
		DoubleWritable val;
		if(!"Price".equals(rowData[2]) && !"Country".equals(rowData[7])){
			val = new DoubleWritable(Integer.parseInt(rowData[2]));
		}else{
			val = new DoubleWritable(0);
		}
		output.collect(new Text(rowData[7]), val);
	}
}
