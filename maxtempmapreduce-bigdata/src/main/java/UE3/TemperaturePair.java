package UE3;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperaturePair implements Writable, WritableComparable<TemperaturePair> {

    private Text yearMonth = new Text();                 // natural key
    private Text day = new Text();
    private DoubleWritable temperature = new DoubleWritable(); // secondary key

   @Override
    /**
     * This comparator controls the sort order of the keys.
     */
    public int compareTo(TemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
                compareValue = temperature.compareTo(pair.getTemperature());
           }
        //return compareValue;    // sort ascending
        return -1*compareValue;   // sort descending
   }

    private DoubleWritable getTemperature() {
        return temperature;
    }

    private BinaryComparable getYearMonth() {
        return yearMonth;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth =  new Text(yearMonth);
    }

    public void setTemperature(double temp) {
        this.temperature = new DoubleWritable(temp);
    }
}
