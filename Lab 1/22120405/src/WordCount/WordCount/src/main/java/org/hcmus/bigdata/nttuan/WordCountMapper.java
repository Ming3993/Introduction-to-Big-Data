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
        String wordCheck = value.toString(); // Extract to Java built-in's String
        if (!wordCheck.matches("^[a-zA-Z]+$")) // Check if the word contains only the alphabets using regex
            return;

        // Get the first character
        // Make it lowercase as the task is counting case-insensitive word
        String startingChar = String.valueOf(wordCheck.charAt(0)).toLowerCase();

        // Eliminate words with unaccepted starting character
        if (!acceptedStartingChar.contains(startingChar))
            return;

        // Set output for the next steps
        keySent.set(startingChar);
        context.write(keySent, valueSent);
    }
}
