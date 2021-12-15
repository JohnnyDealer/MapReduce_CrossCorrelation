package hadoop.mapreduce.patterns.CrossCorrelation.Pairs;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairTuple implements WritableComparable<PairTuple> {
    String elementOne;
    String elementTwo;

    public PairTuple(){

    }

    public PairTuple(String first, String second){
        elementOne = first;
        elementTwo = second;
    }


    @Override
    public int compareTo(PairTuple tuple) {
        int cmpFirst = this.elementOne.compareTo(tuple.elementOne);
        return (cmpFirst != 0) ? cmpFirst : this.elementTwo.compareTo(tuple.elementTwo);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(elementOne);
        dataOutput.writeUTF(elementTwo);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        elementOne = dataInput.readUTF();
        elementTwo = dataInput.readUTF();
    }



    @Override
    public String toString() {
        return String.format("(%s, %s)", elementOne, elementTwo);
    }
}
