package UE4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VSKDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Variance, Skewness, Kurtosis");
        //job.setMapOutputKeyClass(TemperaturePair.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);

        job.setMapperClass(VSKMapper.class);
        job.setReducerClass(VSKReducer.class);
        //job.setPartitionerClass(MedianTempPartitioner.class);
        //job.setSortComparatorClass(MedianKeyComparator.class);


        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new VSKDriver(), args);
        System.exit(exitCode);
    }
}
