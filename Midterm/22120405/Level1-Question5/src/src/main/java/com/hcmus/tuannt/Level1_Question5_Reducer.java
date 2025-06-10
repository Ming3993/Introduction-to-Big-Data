package com.hcmus.tuannt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Level1_Question5_Reducer extends Reducer<IntWritable, Text, Text, Text> {
    @Override
    protected void reduce(IntWritable center, Iterable<Text> points, Context context)
            throws IOException, InterruptedException {
        int centerValue = center.get();
        List<String> pointList = new ArrayList<>();
        double sumValue = 0;

        for (Text point : points) {
            List<String> pointInfo = List.of(point.toString().split(" "));

            pointList.add(pointInfo.get(0));
            sumValue += Integer.parseInt(pointInfo.get(1));
        }

        writeResult(context, sumValue, pointList, centerValue);
    }

    private void writeResult(Context context, double sumValue, List<String> pointList, int centerValue)
            throws IOException, InterruptedException {
        double newCenterValue = sumValue / pointList.size();
        String keyText = centerValue + "\t" + newCenterValue;
        String valueText = String.join(" ", pointList);
        context.write(new Text(keyText), new Text(valueText));
    }
}