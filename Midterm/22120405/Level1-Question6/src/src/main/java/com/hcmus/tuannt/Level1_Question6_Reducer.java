package com.hcmus.tuannt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Level1_Question6_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable distance, Iterable<Text> points, Context context)
            throws IOException, InterruptedException {
        int distanceValue = distance.get();
        List<String> pointList = new ArrayList<>();

        for (Text point : points) {
            pointList.add(point.toString());
        }

        context.write(new IntWritable(distanceValue), new Text(String.join(" ", pointList)));
    }
}
