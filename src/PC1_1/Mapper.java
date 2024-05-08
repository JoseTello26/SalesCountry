package PC1_1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Mapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, DoubleWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] rowData = valueString.split(",");
		DoubleWritable val;
		if(!"Selling Price".equals(rowData[7])){
			val = new DoubleWritable(Integer.parseInt(rowData[7]));
		}else{
			val = new DoubleWritable(0);
		}
		output.collect(new Text("PRICE"), val);
	}
}
