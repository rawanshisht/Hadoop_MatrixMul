import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMulReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context con)
			throws IOException, InterruptedException {
		String[] value;
		Configuration c = con.getConfiguration();
		//Integer[] value;
		float result = 0; 
		int mul = 1;
		int j = Integer.parseInt(c.get("j"));
		//fund same values of j
		for(int x = 1 ; x<=j; x++)
		{
			for(Text val: values)
		{
				value = val.toString().split(",");
				if(x == Integer.parseInt(value[1]))
				{   //then multiply them
					mul*= Integer.parseInt(value[0]);
				}
		}
			//then add the multiplied to result
			result += mul;
			mul = 1;
		}
		con.write(new Text("-"), new Text(key.toString() + "," + Float.toString(result)));
		//the ouput will be - i,j,k, result
		
	//con.write(new Text("hello"), new Text("Reduce"));
		}
		
	}
