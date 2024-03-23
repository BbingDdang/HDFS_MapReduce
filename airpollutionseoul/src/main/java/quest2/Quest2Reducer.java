package quest2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class Quest2Reducer extends Reducer<Text, DoubleWritable, Text, IntWritable>{
	IntWritable oval = new IntWritable();
	int max = 0;
	Text ok = new Text();
	int tmp = 0;
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
					throws IOException, InterruptedException {
		
		int count = 0;
		
		   
		for(DoubleWritable value : values) {
			count += 1;
		}
		
		if (max < count) {
			max = count;
			ok.set(key);
			
		}
		oval.set(max);
	    
	    tmp += 1;
		//oval.set(max);
	    if (tmp == 25) {
	    	context.write(ok, oval);
	    }
	}
	
}