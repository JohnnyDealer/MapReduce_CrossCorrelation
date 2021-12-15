package hadoop.mapreduce.patterns.CrossCorrelation.Pairs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class CrossCorrelationPairs {

    public static class PairsMapper extends Mapper<Object, Text, PairTuple, IntWritable> {

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, PairTuple, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            ArrayList<String> words = new ArrayList<>();
            while(stringTokenizer.hasMoreTokens())
                words.add(stringTokenizer.nextToken());

            for(int i = 0; i < words.size(); i++){
                for(int j = i + 1; j < words.size(); j++){
                    if (words.get(i).compareTo(words.get(j)) > 0)
                        context.write(new PairTuple(words.get(i), words.get(j)), new IntWritable(1));
                    else
                        context.write(new PairTuple(words.get(j), words.get(i)), new IntWritable(1));
                }
            }
        }
    }

    public static class PairsReducer extends Reducer<PairTuple, IntWritable, PairTuple, IntWritable>{

        @Override
        protected void reduce(PairTuple key, Iterable<IntWritable> values, Reducer<PairTuple, IntWritable, PairTuple, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(IntWritable value : values){
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }
}
