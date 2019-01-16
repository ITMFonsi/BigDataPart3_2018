package UE3.Task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

public class SortByTempReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<Text> list = new ArrayList<>();
        for (Text value : values) {
            list.add(value);
        }
        System.out.println(key.toString() + " () " + list.get(0).toString());
        context.write(key,  list.get(0));
    }
}
