import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMulMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context con)
			throws IOException, InterruptedException {
		Configuration c = con.getConfiguration();
		int i = Integer.parseInt(c.get("i"));
		int k = Integer.parseInt(c.get("k"));
		String line = value.toString();
		String[] indexAndValue = line.split(",");
		Text outputKey = new Text();
		Text outputValue = new Text();
		//if values are from matrix a, then loop over k and put the key and value as (i,x) => (value, j)
		if(indexAndValue[0].equals("A")){
			for(int x=1; x<=k; x++){
				outputKey.set(indexAndValue[1]+ "," + x);
				outputValue.set(indexAndValue[3] + "," + indexAndValue[2]);
				con.write(outputKey, outputValue);
			}
		}else{
			//else (x,k)=> (value, j)
			for(int x=0; x<i; x++){
				outputKey.set(x+ "," + indexAndValue[2]);
				outputValue.set(indexAndValue[3] + "," + indexAndValue[1]);
				con.write(outputKey, outputValue);
			}
		}
	}

}
