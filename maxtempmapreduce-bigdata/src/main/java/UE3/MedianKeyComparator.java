package UE3;

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

        int cmp = p1.compareTo(p2);
        if (cmp != 0) {
            return cmp;
        }
        return -p1.compareTo(p2);
    }
}
