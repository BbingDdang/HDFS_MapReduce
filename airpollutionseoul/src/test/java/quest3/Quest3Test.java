package quest3;

import org.apache.hadoop.util.ToolRunner;

public class Quest3Test {
	public static void main(String[] not_used) throws Exception {
		
		String[] args = {"src/test/resources/Measurement_info.csv"};
		ToolRunner.run(new Quest3(), args);
		}
}
