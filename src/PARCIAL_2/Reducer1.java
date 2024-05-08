package PARCIAL_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;



public class Reducer1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		Text key = t_key;
		int freq = 0;
		double sum = 0;
		List<Double> list = new ArrayList<Double>();
		List<double[]> pesos = new ArrayList<>();

		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			Text value = values.next();
			double[] vals = Arrays.stream(value.toString().split(",")).mapToDouble(Double::parseDouble).toArray();
			pesos.add(Arrays.copyOfRange(vals, 0, vals.length-1));
			list.add(vals[vals.length-1]);
		}

		int maxIndex = list.indexOf(Collections.max(list));

		String w = "";

		for(int i=0; i<pesos.get(maxIndex).length; ++i){
			if(i==0){
				w = Double.toString(pesos.get(maxIndex)[i]);
			}
			else{
				w = w + "," + pesos.get(maxIndex)[i];
			}
		}



		/*
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


		 */
		output.collect(new Text(w), new Text(Double.toString(list.get(maxIndex))));
	}
}