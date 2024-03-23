package quest4;

import java.io.IOException;
import java.util.StringTokenizer;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Quest4Reducer extends Reducer<Text, Text, Text, Text> {
	
    Text oval = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String value = Joiner.on("").join(values);
        String []tokens = value.split("\t");
        
        String[] name = {"SO2", "NO2", "CO", "O3", "PM10", "PM2.5"};
        int[] id = {1, 3, 5, 6, 8, 9};
        double[] sumlist = {0,0,0,0,0,0};
        int[] countlist = {0,0,0,0,0,0};
        String[] result = {"", "", "", "", "", ""};
     

        for(int i=0; i < tokens.length; i++) {
            StringTokenizer st = new StringTokenizer(tokens[i], ","); 
            String item = st.nextToken();
            double pm = Double.parseDouble(st.nextToken());
            if (pm > 0) {
                if (Integer.parseInt(item) == id[0]) {
                    sumlist[0] += pm;
                    countlist[0] += 1;
                } else if (Integer.parseInt(item) == 3) {
                	sumlist[1] += pm;
                    countlist[1] += 1;
                } else if (Integer.parseInt(item) == 5) {
                	sumlist[2] += pm;
                    countlist[2] += 1;
                } else if (Integer.parseInt(item) == 6) {
                	sumlist[3] += pm;
                    countlist[3] += 1;
                } else if (Integer.parseInt(item) == 8) {
                	sumlist[4] += pm;
                    countlist[4] += 1;
                } else if (Integer.parseInt(item) == 9) {
                	sumlist[5] += pm;
                    countlist[5] += 1;
                }
            }
        }
        for(int j = 0; j < id.length; j++) {
        	sumlist[j] = sumlist[j]/countlist[j];
        	result[j] = Double.toString(sumlist[j]);
        }
        

        oval.set("<"+name[0]+" : "+result[0]+", "+name[1]+" : "+result[1]+", "+name[2]+" : "+result[2]+", "+name[3]+" : "+result[3]+", "+name[4]+" : "+result[4]+", "+name[5]+" : "+result[5]+">");
        context.write(key, oval);

    }
    
}
