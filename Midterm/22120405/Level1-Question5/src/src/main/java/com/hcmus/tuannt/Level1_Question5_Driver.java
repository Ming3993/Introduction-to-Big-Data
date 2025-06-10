package com.hcmus.tuannt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Random;

public class Level1_Question5_Driver {
    public static void main(String[] args)
            throws Exception {
        // Check the number of arguments
        if (args.length != 2){
            System.err.println("Usage: <executable> <input-file> <output-file>");
            System.exit(1);
        }

        // Generate three random centers
        Configuration config = setupConfigurationWithGeneratedRandomNumbers();

        // Set up a job
        Job job = Job.getInstance(config, "Level 1 - Question 5");
        job.setJarByClass(Level1_Question5_Driver.class);

        job.setMapperClass(Level1_Question5_Mapper.class);
        job.setReducerClass(Level1_Question5_Reducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Configuration setupConfigurationWithGeneratedRandomNumbers() {
        Random rand =  new Random();
        int center1 = rand.nextInt(2001) - 1000; // -1000 to 1000

        int center2;
        do {
            center2 = rand.nextInt(2001) - 1000; // -1000 to 1000
        } while (center1 == center2);

        int center3;
        do {
            center3 = rand.nextInt(2001) - 1000; // -1000 to 1000
        } while (center1 == center3 || center2 == center3);

        // Set up configuration
        Configuration config = new Configuration();
        config.setInt("center1", center1);
        config.setInt("center2", center2);
        config.setInt("center3", center3);
        return config;
    }
}