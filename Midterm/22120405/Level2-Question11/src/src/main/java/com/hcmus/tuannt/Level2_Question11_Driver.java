package com.hcmus.tuannt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Level2_Question11_Driver {
    public static void main(String[] args)
            throws Exception {
        // Check the number of arguments
        if (args.length != 2){
            System.err.println("Usage: <executable> <input-file> <output-file>");
            System.exit(1);
        }

        // Set up configuration
        Configuration config = new Configuration();

        // Set up a job
        Job job = Job.getInstance(config, "Level 2 - Question 11");
        job.setJarByClass(Level2_Question11_Driver.class);

        job.setMapperClass(Level2_Question11_Mapper.class);
        job.setReducerClass(Level2_Question11_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}