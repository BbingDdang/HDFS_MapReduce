package quest1;

import org.apache.hadoop.util.ToolRunner;

import quest1.Quest1;

public class Quest1Test {
	public static void main(String[] not_used) throws Exception {
		
		String[] args = {"src/test/resources/Measurement_info.csv"};
		ToolRunner.run(new Quest1(), args);
		}
}
