package quest3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class Quest3Mapper extends Mapper<Object, Text, Text, Text>{
	Text word = new Text();
	Text one = new Text();
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		/* 파이썬의 split 느낌 */
		StringTokenizer st = new StringTokenizer(value.toString(),",");
		String time = st.nextToken();
		String station = st.nextToken();
		if(station.startsWith("Station")) {
			return;
		}
		String item = st.nextToken();
		String pm = st.nextToken();
		StringTokenizer tmp = new StringTokenizer(time, " ");
		String date = tmp.nextToken();
		String hourminute = tmp.nextToken();
		StringTokenizer hm = new StringTokenizer(hourminute, ":");
		String h = hm.nextToken();
		String m = hm.nextToken();
		if (h.length() == 1) {
			h = "0" + h;
		}
		hourminute = h + ":" + m;
		time = date + " " + hourminute;
		
		/*item이 8이어야 pm10* 9여야 pm2.5*/
		//System.out.println(item + one);
		word.set("<" + time + ", " + station +  ">");
		one.set("("+item+", "+pm+")");
		//word.set(time + station);
		//one.set(item + pm);
		
		context.write(word, one);
		
	
	}
	
}