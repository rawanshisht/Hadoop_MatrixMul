

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Driver {
	public static void main(String [] args) throws Exception{
		Configuration c = new Configuration();
		//Matrix A: (i,j), Matrix B: (j,k)
		// set the rows and coloumns of the matrix
		c.set("i", "3");
		c.set("j", "3");
		c.set("k", "3");
		String[] files = new GenericOptionsParser(c,args).getRemainingArgs();
		//get the input and output from args
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job job = new Job(c, "MatrixMul");
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(MatrixMulMapper.class);
		job.setReducerClass(MatrixMulReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		System.exit(job.waitForCompletion(true)?0 : 1);
	}

}
