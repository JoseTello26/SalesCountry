package PC1_2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ResultsWriteable implements Writable {
    double mean;
    double sd;
    double median;
    double min;
    double max;

    public void readFields(DataInput in) throws IOException {
        mean = in.readDouble();
        sd = in.readDouble();
        median = in.readDouble();
        min = in.readDouble();
        max = in.readDouble();
    }
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.mean);
        out.writeDouble(this.sd);
        out.writeDouble(this.median);
        out.writeDouble(this.min);
        out.writeDouble(this.max);
    }
    @Override
    public String toString() {
        return mean + "\t" + sd + "\t" + median + "\t" + min + "\t" +max;
    }
}