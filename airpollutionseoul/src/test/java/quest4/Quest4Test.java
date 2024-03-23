package quest4;


import org.apache.hadoop.util.ToolRunner;

public class Quest4Test {
    public static void main(String[] args) throws Exception {

        String[] myargs = {"src/test/resources/Measurement_info.csv"};

        ToolRunner.run(new Quest4(), myargs);
    }
}