package org.hcmus.bigdata.nttuan;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Count number of the same keys
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Set output for the next steps
        context.write(key, new IntWritable(sum));
    }
}
