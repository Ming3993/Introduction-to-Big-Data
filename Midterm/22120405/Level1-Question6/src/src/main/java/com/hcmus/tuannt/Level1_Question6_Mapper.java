package com.hcmus.tuannt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Level1_Question6_Mapper extends Mapper<Text, Text, IntWritable, Text> {
    int queryPoint;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        queryPoint = conf.getInt("query.point", 0);
    }

    @Override
    protected void map(Text pointName, Text pointCoordination, Context context)
            throws IOException, InterruptedException {
        int pointValue = Integer.parseInt(pointCoordination.toString());
        int distance = Math.abs(queryPoint - pointValue);
        context.write(new IntWritable(distance), pointName);
    }
}