package org.hcmus.bigdata.nttuan;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    // Create in advance IntWritable to be sent
    // Initialize as 1
    private final static IntWritable valueSent = new IntWritable(1);

    private final static Text keySent = new Text();

    // Create a list of accepted starting character
    private final static List<String> acceptedStartingChar = List.of("a", "f", "j", "g", "h", "c", "m", "u", "s");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); // Extract to Java built-in's String

        // Split the line based on non-alphabetic character
        String[] words = line.split("[^A-Za-z]+");

        for (String word : words) {
            if (word.isEmpty())
                continue;

            // Get the first character
            // Make it lowercase as the task is counting case-insensitive line
            String startingChar = String.valueOf(word.charAt(0)).toLowerCase();

            // Eliminate words with unaccepted starting character
            if (!acceptedStartingChar.contains(startingChar))
                continue;

            // Set output for the next steps
            keySent.set(startingChar);
            context.write(keySent, valueSent);
        }
    }
}
