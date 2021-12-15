package hadoop.mapreduce.patterns.OrderCart;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class OrderCart {
    private final Random random;

    private static final String outputLocalPath = "/home/petya/Files/Hadoop/mydata/CrossCorrelation/order-cart";

    public OrderCart(){
        this.random = new Random();
    }

    public void GenerateOrderCart() throws IOException {
        Products[] products = Products.values();
        ArrayList<Products> cart;

        StringBuilder order = new StringBuilder();

        for(int i = 0; i < random.nextInt(20) + 20; i++){
            cart = new ArrayList<>();

            for(int j = 0; j < random.nextInt(20) + 20; j++){
                int tempIndex = random.nextInt(products.length);
                while(cart.contains(products[tempIndex])){
                    tempIndex = random.nextInt(products.length);
                }
                cart.add(products[tempIndex]);
            }

            for(Products p : cart)
                order.append(p.name()).append(" ");
            order.append("\n");
        }

        this.writeFile(order.toString());
    }


    private void writeFile(String value) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputLocalPath));
        writer.write(value);

        writer.close();
    }

}
