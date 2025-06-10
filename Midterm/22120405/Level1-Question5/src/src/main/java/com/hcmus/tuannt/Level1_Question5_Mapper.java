package com.hcmus.tuannt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Level1_Question5_Mapper extends Mapper<Text, Text, IntWritable, Text> {
    private static final String[] CENTER_KEYS = {"center1", "center2", "center3"};
    private List<Integer> centerList;
    private final Random random = new Random();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        initializeCenters(context.getConfiguration());
    }

    private void initializeCenters(Configuration conf) {
        centerList = Arrays.stream(CENTER_KEYS)
                .map(key -> conf.getInt(key, 0))
                .collect(Collectors.toList());
    }

    @Override
    protected void map(Text pointName, Text pointCoordination, Context context)
            throws IOException, InterruptedException {
        System.out.println(pointName + " " + pointCoordination);
        int pointValue = Integer.parseInt(pointCoordination.toString());
        int selectedCenter = findFarthestCenter(pointValue);
        writeResult(pointName.toString(), pointValue, selectedCenter, context);
    }

    private int findFarthestCenter(int pointValue) {
        Integer minDistance = null;
        List<Integer> equalMinDistanceList = new ArrayList<>();

        for (int center : centerList) {
            int distance = Math.abs(center - pointValue);

            if (minDistance == null) {
                minDistance = distance;
                equalMinDistanceList.add(center);
            }

            if (minDistance == distance)
                equalMinDistanceList.add(center);

            if (distance < minDistance) {
                equalMinDistanceList.clear();
                equalMinDistanceList.add(center);
                minDistance = distance;
            }
        }

        return selectRandomCenter(equalMinDistanceList);
    }

    private int selectRandomCenter(List<Integer> centers) {
        return centers.get(random.nextInt(centers.size()));
    }

    private void writeResult(String pointName, Integer pointValue, int center, Context context)
            throws IOException, InterruptedException {
        String serializedPointInfo = pointName + " " + pointValue.toString();
        context.write(new IntWritable(center), new Text(serializedPointInfo));
    }
}