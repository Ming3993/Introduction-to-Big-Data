package com.hcmus.tuannt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Level1_Question6_Driver {
    public static void main(String[] args)
            throws Exception {
        // Check the number of arguments
        if (args.length != 3){
            System.err.println("Usage: <executable> <input-file> <output-file> <query-point>");
            System.exit(1);
        }

        // Set up configuration
        Configuration config = new Configuration();
        config.set("query.point", args[2]);

        // Set up a job
        Job job = Job.getInstance(config, "Level 1 - Question 6");
        job.setJarByClass(Level1_Question6_Driver.class);

        job.setMapperClass(Level1_Question6_Mapper.class);
        job.setReducerClass(Level1_Question6_Reducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}