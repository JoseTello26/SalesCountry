package PC1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class ResultsWriteable implements Writable {
    double mean;
    double median;
    double sum;

    public void readFields(DataInput in) throws IOException {
        mean = in.readDouble();
        median = in.readDouble();
        sum = in.readDouble();
    }
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.mean);
        out.writeDouble(this.median);
        out.writeDouble(this.sum);
    }
    @Override
    public String toString() {
        return "TOTAL: "+sum;
    }
}