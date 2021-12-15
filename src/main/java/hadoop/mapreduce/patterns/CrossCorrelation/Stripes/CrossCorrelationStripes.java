package hadoop.mapreduce.patterns.CrossCorrelation.Stripes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class CrossCorrelationStripes {

    public static class StripesMapper extends Mapper<Object, Text, Text, MapWritable>{

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, MapWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            ArrayList<String> words = new ArrayList<>();
            while(stringTokenizer.hasMoreTokens())
                words.add(stringTokenizer.nextToken());

            for(int i = 0; i < words.size(); i++){
                MapWritable map = new MapWritable();

                for(int j = 0; j < words.size(); j++)
                    if(i != j)
                        map.merge(new Text(words.get(j)), new IntWritable(1),
                                (oldValue, newValue) -> new IntWritable(Integer.sum((((IntWritable) oldValue).get()), (((IntWritable) newValue).get()))));

                context.write(new Text(words.get(i)), map);
            }
        }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable>{

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Reducer<Text, MapWritable, Text, MapWritable>.Context context) throws IOException, InterruptedException {
            MapWritable map = new MapWritable();

            for(MapWritable mapWritable : values)
                for(Writable k : mapWritable.keySet())
                    map.merge(k, mapWritable.get(k), (oldValue, newValue) -> new IntWritable(Integer.sum((((IntWritable) oldValue).get()), (((IntWritable) newValue).get()))));

            context.write(key, map);
        }
    }

}
