package PC1;

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



public class Reducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, DoubleWritable, Text, ResultsWriteable> {

	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, ResultsWriteable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		String[] keys = key.toString().split("_");
		int freq = Integer.parseInt(keys[1]);
		double sum = 0;
		if (keys[0].equals("Country")){
			return;
		}
		List<Double> list = new ArrayList<Double>();
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			DoubleWritable value = (DoubleWritable) values.next();
			sum += value.get();
		}
		double mean=sum/(freq);

		Collections.sort(list);
		double median=0;
		if (freq % 2 == 0) {
			double medianSum = list.get((freq / 2) -1) + list.get(freq / 2);
			median = medianSum / 2;
		} else {
			median = list.get(freq / 2);
		}
		ResultsWriteable res = new ResultsWriteable();
		res.mean = mean;


		output.collect(key, res);
	}
}