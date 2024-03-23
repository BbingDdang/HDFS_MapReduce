package quest2;

import org.apache.hadoop.util.ToolRunner;

public class Quest2Test {
	public static void main(String[] not_used) throws Exception {
		
		String[] args = {"src/test/resources/Measurement_info.csv"};
		ToolRunner.run(new Quest2(), args);
		}
}
