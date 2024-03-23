package quest2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class Quest2Mapper extends Mapper<Object, Text, Text, DoubleWritable>{
	Text word = new Text();
	DoubleWritable one = new DoubleWritable();
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		/* 파이썬의 split 느낌 */
		StringTokenizer st = new StringTokenizer(value.toString(),",");
		st.nextToken();
		String station = st.nextToken();
		if(station.startsWith("Station")) {
			return;
		}
		String item = st.nextToken();
		Double pm = Double.parseDouble(st.nextToken());
		
		one.set(pm);
		
		String status = st.nextToken();
		/*item이 8이어야 pm10* 9여야 pm2.5*/
		//System.out.println(item + one);
		if ((Integer.parseInt(item) == 8 && pm <= 30) || (Integer.parseInt(item) == 9 && pm <= 15)) {
			
			if (Integer.parseInt(status) == 0) {
				
				word.set(station);
				
				context.write(word, one);
				//System.out.println("aft" + one);
			}
		}
		
		
		
	
	}
	
}
