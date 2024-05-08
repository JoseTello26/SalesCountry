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

public class Combiner extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int freq = 0;
        double sum = 0;
        if (key.equals("Country")){
            return;
        }
        List<Double> list = new ArrayList<Double>();
        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            DoubleWritable value = (DoubleWritable) values.next();
            sum += value.get();
            list.add(value.get());
            freq++;
        }

        output.collect(new Text(key.toString() + "_" + freq), new DoubleWritable(sum));
    }
}
