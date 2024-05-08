package PC1_1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;



public class Reducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, DoubleWritable, Text, ResultsWriteable> {

	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text,ResultsWriteable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		int freq = 0;
		double sum = 0;
		List<Double> list = new ArrayList<Double>();
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			DoubleWritable value = (DoubleWritable) values.next();
			sum += value.get();
			list.add(value.get());
			freq++;
		}
		double mean=sum/(freq-1);

		double sumOfSquares = 0;
		for (double i : list) {
			sumOfSquares += (i - mean) * (i - mean);
		}
		double sd = Math.sqrt(sumOfSquares/(freq-1));

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
		res.sd = sd;
		res.median = median;
		res.min = list.get(0);
		res.max = list.get(freq - 1);

		output.collect(new Text("Mean, Std. Dev, Median, Min, Max"), res);
	}
}