package quest3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class Quest3Reducer extends Reducer<Text, Text, Text, Text>{
	Text oval = new Text();
	
	Text ok = new Text();
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
		
		String data = "";
		for(Text value : values) {
			String d = value.toString();
			data += d + " ";
		}
		
	    
		oval.set(data);
	    
    	context.write(key, oval);
	    
	}
	
}