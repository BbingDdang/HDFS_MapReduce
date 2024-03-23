package quest1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class Quest1Reducer extends Reducer<Text, DoubleWritable, Text, Text>{
	Text oval = new Text();
	
	
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
					throws IOException, InterruptedException {
		/*변수 초기화*/
		double sum = 0;
		int count = 0;
		/*value 값 범위 0<=value<=124.14 -1일땐 pass*/
		double min = Double.POSITIVE_INFINITY;
		double max = Double.NEGATIVE_INFINITY;
		for(DoubleWritable value : values) {
			double tmp = value.get();
			//System.out.println("tmp"+value.get());
			
			if (tmp < 0) {
				continue;
			}
			
			if (tmp < min) {
				min = tmp;
			}
			if (tmp > max){
				max = tmp;
			}
			
			sum += tmp;
			count += 1;
		}
		
		double average = sum / count;
		oval.set(max + "," + min + "," + average);
		
		//oval.set(max);
		context.write(key, oval);
	}
}