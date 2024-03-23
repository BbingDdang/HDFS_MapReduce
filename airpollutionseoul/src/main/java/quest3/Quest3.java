package quest3;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Quest3 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Quest3(), args);

    }

    public int run(String[] args) throws Exception {
        
        Job job = Job.getInstance(getConf());
        job.setJarByClass(Quest3.class);
        job.setMapperClass(Quest3Mapper.class);
        job.setReducerClass(Quest3Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[0] + ".out3"));

        job.waitForCompletion(true);
        


        return 0;
    }


}