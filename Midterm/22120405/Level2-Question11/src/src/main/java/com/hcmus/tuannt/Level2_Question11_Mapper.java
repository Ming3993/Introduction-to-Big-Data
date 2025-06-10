package com.hcmus.tuannt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Level2_Question11_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] tokens = line.toString().split(" ");
        if (tokens.length < 3)
            return;

        String key = tokens[0];
        String foodName = tokens[1];
        String value = tokens[2];

        context.write(new Text(foodName), new Text(key + " " + value));
    }
}