package UE3.Task1;

import UE3.TemperaturePair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MedianKeyComparator extends WritableComparator {


    protected MedianKeyComparator() {
        super(TemperaturePair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TemperaturePair p1 = (TemperaturePair) a;
        TemperaturePair p2 = (TemperaturePair) b;

        int compareValue = p1.getYearMonth().compareTo(p2.getYearMonth());
        if (compareValue == 0) {
           compareValue = p1.getTemperature().compareTo(p2.getTemperature());
        }
        return compareValue;
    }
}
