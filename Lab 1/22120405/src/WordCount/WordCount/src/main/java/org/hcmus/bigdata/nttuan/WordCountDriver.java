package org.hcmus.bigdata.nttuan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Load default configuration in core-site.xml
        Configuration conf = new Configuration();

        // Create and set up a new job
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class); // Set the jar file that nodes will look up for necessary classes
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setSortComparatorClass(WordCountComparator.class);

        // Set up output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output path based on the user's parameters
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job, the job is now distributed over the nodes to run
        job.submit();
        System.exit(job.waitForCompletion(true) ? 0 : 1); // Wait for the MapReduce task to end
    }
}
