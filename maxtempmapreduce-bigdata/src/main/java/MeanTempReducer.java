import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MeanTempReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text text, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        Iterator iterator = values.iterator();
        double sum = 0.0;
        int count = 0;
        while(iterator.hasNext()) {
            sum = sum + ((DoubleWritable)iterator.next()).get();
            count++;
        }
        sum = sum/10;
        context.write(text, new DoubleWritable(sum/count));
    }
}
