package com.hcmus.tuannt;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Level2_Question11_Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text food, Iterable<Text> foodInfos, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> foodMap = new HashMap<>();
        for (Text foodInfo : foodInfos) {
            String[] tokens = foodInfo.toString().split(" ");
            String foodKey = tokens[0];
            String foodValue = tokens[1];

            foodMap.put(foodKey, Integer.parseInt(foodValue));
        }

        if (foodMap.containsKey("FoodPrice") && foodMap.containsKey("FoodQuantity")) {
            int foodPrice = foodMap.get("FoodPrice");
            int foodQuantity = foodMap.get("FoodQuantity");
            context.write(food, new Text(foodPrice + "\t" + foodQuantity));
        }
    }
}
