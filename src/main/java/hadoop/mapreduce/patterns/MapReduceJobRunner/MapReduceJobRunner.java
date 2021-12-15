package hadoop.mapreduce.patterns.MapReduceJobRunner;

import hadoop.mapreduce.patterns.CrossCorrelation.Pairs.CrossCorrelationPairsJob;
import hadoop.mapreduce.patterns.CrossCorrelation.Stripes.CrossCorrelationStripesJob;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceJobRunner {

    public MapReduceJobRunner(){

    }

    public static int RunCrossCorrelationPairsJob(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CrossCorrelationPairsJob(), args);

        return exitCode;
    }

    public static int RunCrossCorrelationStripesJob(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CrossCorrelationStripesJob(), args);

        return exitCode;
    }

}
