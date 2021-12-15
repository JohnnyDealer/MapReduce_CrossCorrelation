package hadoop.mapreduce.patterns.CrossCorrelation.Stripes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class CrossCorrelationStripesJob extends Configured implements Tool {
    private static final String jobName = "CrossCorrelationStripes";
    private static final String inputPath = "hdfs://localhost:9000/user/petya/mydata/CrossCorrelation/stripes/input";
    private static final String outputPath = "hdfs://localhost:9000/user/petya/mydata/CrossCorrelation/stripes/output";

    private static final String coreSitePath = "/home/petya/Work/Hadoop/hadoop-2.10.1/etc/hadoop/core-site.xml";
    private static final String hdfsSitePath = "/home/petya/Work/Hadoop/hadoop-2.10.1/etc/hadoop/hdfs-site.xml";

    @Override
    public int run(String[] strings) throws Exception {
        // Объявляем job
        Configuration configuration = getConf();
        configuration.addResource(new Path(coreSitePath));
        configuration.addResource(new Path(hdfsSitePath));

        Job crossCorrelationStripesJob = Job.getInstance(configuration, jobName);
        crossCorrelationStripesJob.setJarByClass(getClass());


        // Назначаем путь к входным данным (файлам)
        FileInputFormat.setInputPaths(crossCorrelationStripesJob, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(crossCorrelationStripesJob, false);

        // Назначаем путь к выходным данным
        FileOutputFormat.setOutputPath(crossCorrelationStripesJob, new Path(outputPath));

        crossCorrelationStripesJob.setMapperClass(CrossCorrelationStripes.StripesMapper.class);
        crossCorrelationStripesJob.setReducerClass(CrossCorrelationStripes.StripesReducer.class);

        crossCorrelationStripesJob.setMapOutputKeyClass(Text.class);
        crossCorrelationStripesJob.setMapOutputValueClass(MapWritable.class);
        crossCorrelationStripesJob.setOutputKeyClass(Text.class);
        crossCorrelationStripesJob.setOutputValueClass(MapWritable.class);

        return crossCorrelationStripesJob.waitForCompletion(true) ? 0 : 1;
    }
}
