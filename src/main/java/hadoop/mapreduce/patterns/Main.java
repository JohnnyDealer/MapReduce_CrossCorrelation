package hadoop.mapreduce.patterns;


import hadoop.mapreduce.patterns.MapReduceJobRunner.MapReduceJobRunner;
import hadoop.mapreduce.patterns.OrderCart.OrderCart;

public class Main {

    public static void main(String[] args) throws Exception {
        //MapReduceJobRunner.RunCrossCorrelationPairsJob(args);
        MapReduceJobRunner.RunCrossCorrelationStripesJob(args);

        //OrderCart orderCart = new OrderCart();
        //orderCart.GenerateOrderCart();
    }
}
