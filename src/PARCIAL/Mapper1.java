package PARCIAL;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Mapper1 extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] rowData = valueString.split(",");
		DoubleWritable val;
		String[] c = {"Age", "Sex", "Bilirubin", "Cholesterol", "Tryglicerides", "Stage"};
		List<String> cols_name = Arrays.asList(c);
		int[] cols = {3, 4, 9, 10, 15};
		String vector = "";

		if(!cols_name.contains(rowData[3])) {
			for(int i : cols ){
				if(i==3){
					vector = rowData[i];
				}
				else if(rowData[i].equals("F")){
					vector = vector + "," + "0";
				}
				else if(rowData[i].equals("M")){
					vector = vector + "," + "1";
				}
				else{
					vector = vector + "," + rowData[i];
				}
			vector= vector + "," + rowData[18];
			}
		}else{
			vector += "0";
		}


		output.collect(new Text("Parametros"), new Text(vector));
	}
}
