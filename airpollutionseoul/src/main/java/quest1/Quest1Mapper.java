package quest1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class Quest1Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
	Text word = new Text();
	DoubleWritable one = new DoubleWritable();
	
	/*value 줄 형식 2017-01-01 00:00,101,1,0.004,0 */
	/*key를 지역 value를 0.004 PM 으로 설정*/
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
		if(item.startsWith("Item")) {return;}
		
		Double pm10 = Double.parseDouble(st.nextToken());
		
		one.set(pm10);
		
		String status = st.nextToken();
		/*item이 8이어야 pm10*/
		//System.out.println(item + one);
		if (Integer.parseInt(item) == 8) {
			
			if (Integer.parseInt(status) == 0) {
				
				word.set(station);
				
				context.write(word, one);
				//System.out.println("aft" + one);
			}
		}
		
		
		
	
	}
}
