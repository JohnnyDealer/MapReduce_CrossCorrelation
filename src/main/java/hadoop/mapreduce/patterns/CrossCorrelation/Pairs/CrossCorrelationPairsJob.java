package hadoop.mapreduce.patterns.CrossCorrelation.Pairs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class CrossCorrelationPairsJob extends Configured implements Tool {
    private static final String jobName = "CrossCorrelationPairs";
    private static final String inputPath = "hdfs://localhost:9000/user/petya/mydata/CrossCorrelation/pairs/input";
    private static final String outputPath = "hdfs://localhost:9000/user/petya/mydata/CrossCorrelation/pairs/output";

    private static final String coreSitePath = "/home/petya/Work/Hadoop/hadoop-2.10.1/etc/hadoop/core-site.xml";
    private static final String hdfsSitePath = "/home/petya/Work/Hadoop/hadoop-2.10.1/etc/hadoop/hdfs-site.xml";

    @Override
    public int run(String[] strings) throws Exception {
        // Объявляем job
        Configuration configuration = getConf();
        configuration.addResource(new Path(coreSitePath));
        configuration.addResource(new Path(hdfsSitePath));

        Job crossCorrelationPairsJob = Job.getInstance(configuration, jobName);
        crossCorrelationPairsJob.setJarByClass(getClass());


        // Назначаем путь к входным данным (файлам)
        FileInputFormat.setInputPaths(crossCorrelationPairsJob, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(crossCorrelationPairsJob, false);

        // Назначаем путь к выходным данным
        FileOutputFormat.setOutputPath(crossCorrelationPairsJob, new Path(outputPath));

        crossCorrelationPairsJob.setMapperClass(CrossCorrelationPairs.PairsMapper.class);
        crossCorrelationPairsJob.setReducerClass(CrossCorrelationPairs.PairsReducer.class);

        crossCorrelationPairsJob.setMapOutputKeyClass(PairTuple.class);
        crossCorrelationPairsJob.setMapOutputValueClass(IntWritable.class);
        crossCorrelationPairsJob.setOutputKeyClass(PairTuple.class);
        crossCorrelationPairsJob.setOutputValueClass(IntWritable.class);

        return crossCorrelationPairsJob.waitForCompletion(true) ? 0 : 1;
    }
}
