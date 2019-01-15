package UE3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MedianGroupComparator extends WritableComparator {

    public MedianGroupComparator() {
        super(TemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable tp1, WritableComparable tp2) {
        TemperaturePair temperaturePair = (TemperaturePair) tp1;
        TemperaturePair temperaturePair2 = (TemperaturePair) tp2;

        return temperaturePair.compareTo(temperaturePair2);
    }
}
