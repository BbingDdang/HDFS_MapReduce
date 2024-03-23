package quest4;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Quest4Mapper extends Mapper<Object, Text, Text, Text> {

    Text ok = new Text();
    Text ov = new Text();


    @Override
    protected void map(Object key, Text value,
                       Context context)
            throws IOException, InterruptedException {

        StringTokenizer st = new StringTokenizer(value.toString(), " ");
        st.nextToken();
        String tmp = st.nextToken();
        
        StringTokenizer st2 = new StringTokenizer(tmp, ",");
        String time = st2.nextToken();
        String station = st2.nextToken();
        if(station.startsWith("Station")) {
			return;
		}
        String item = st2.nextToken(); 
        String pm = st2.nextToken();
        
        StringTokenizer hm = new StringTokenizer(time, ":");
        String h = hm.nextToken();
        String m = hm.nextToken();
        if (h.length() == 1) {
        	h = "0" + h;
        }
        time = h + ":" + m;
        ok.set(time);
        ov.set(item + "," + pm + " ");

        context.write(ok, ov);

    }

}