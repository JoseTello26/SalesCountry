
package PARCIAL_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
	public int run(String[] args) throws Exception{
		JobClient my_client = new JobClient();
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(Driver.class);
		// Set a name of the Job
		job_conf.setJobName("NeuralNetwork");
		// Specify data type of output key and value
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(Text.class);
		// Specify names of Mapper and Reducer Class

		job_conf.setMapperClass(Mapper1.class);
		job_conf.setCombinerClass(Combiner.class);
		job_conf.setReducerClass(Reducer1.class);

		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);
		// Set input and output directories using command line arguments,
		//arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));


		JobClient.runJob(job_conf);
		return 0;
	}

	public static void main(String[] args) {

		try {
			// Run the job
			int res = ToolRunner.run(new Configuration(), new Driver(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
