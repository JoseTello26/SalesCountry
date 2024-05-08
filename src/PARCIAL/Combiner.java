package PARCIAL;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Combiner extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;

        List<double[]> list_i = new ArrayList<double[]>();
        List<double[]> list_o = new ArrayList<double[]>();

        while (values.hasNext()) {
            double[] i = {};
            // replace type of value with the actual type of our value
            Text value = values.next();
            if(!value.toString().equals("0")){
                i = Arrays.stream(value.toString().split(",")).mapToDouble(Double::parseDouble).toArray();
                System.out.println(i.length);
                list_i.add(Arrays.copyOfRange(i, 0, 5));
                double[] v = Arrays.copyOfRange(i, 5, 6);
                //sum += value.get();
                list_o.add(v);
            }
            //freq++;
        }
        double[][] out = new double[list_o.size()][1];
        double[][] input = new double[list_i.size()][];
        for(int i=0; i<list_o.size(); ++i){
            out[i] = list_o.get(i);
        }
        for(int i=0; i<list_i.size(); ++i){
            input[i] = list_i.get(i);
        }

        rna01 rn = new rna01(5, 2, 1);
        rn.entrenamiento(input, out,1000);
        double acc = rn.prueba(input, out);

        String pesos = "";

        for(int i=0; i<rn.w.length; ++i){
            if(i==0){
                pesos = Double.toString(rn.w[i]);
            }
            else{
                pesos = pesos + "," + Double.toString(rn.w[i]);
            }
        }

        pesos = pesos +","+ acc;

        output.collect(new Text("Pesos"), new Text(pesos));
    }
}
