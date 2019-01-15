package UE3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MedianYearMonthComparator extends WritableComparator {

    public MedianYearMonthComparator() {
        super(TemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TemperaturePair pair1 = (TemperaturePair) a;
        TemperaturePair pair2 = (TemperaturePair) b;
        return pair1.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
