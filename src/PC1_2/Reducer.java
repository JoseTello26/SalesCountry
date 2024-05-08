package PC1_2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;



public class Reducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Text key = t_key;

		String res = new String();
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			String val = values.next().toString();
			if(!val.isEmpty()){
				res = res +"\n" + val;
			}
		}

		output.collect(new Text(key), new Text(res));
	}
}