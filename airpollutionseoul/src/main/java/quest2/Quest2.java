package quest2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

public class Quest2 extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Quest2(), args);
	}
	
	
	public int run(String[] args) throws Exception {
		
		Job myjob = Job.getInstance(getConf());
		myjob.setJarByClass(Quest2.class);
		myjob.setMapperClass(Quest2Mapper.class);
		myjob.setReducerClass(Quest2Reducer.class);
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(DoubleWritable.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		myjob.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[0]+".out2"));
		myjob.waitForCompletion(true);
		return 0;
	}
	
	
}