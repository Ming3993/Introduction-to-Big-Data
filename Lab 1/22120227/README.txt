1. Introduction

This project uses Hadoop Streaming to perform a word count task on a given text file. The goal is to count words that do not contain any special characters, according to the provided requirements.

The MapReduce program consists of two scripts:
- Mapper.py: Processes each line, extracts words, and emits the first letter if the word contains only alphabets.
- Reducer.py: Aggregates the counts for each letter and outputs the final results.

2. Setup
Upload file to HDFS
- Create directories on HDFS:

	hdfs dfs -mkdir -p /WordCount/input

- Upload input file:

	hdfs dfs -put words.txt /WordCount/input

3. Find the Hadoop Streaming JAR file
To locate the correct Hadoop Streaming JAR file on your system, run the following command:

	find / -name "hadoop-streaming*.jar" 2>/dev/null

This command searches the filesystem for the JAR file and suppresses permission errors.

3. Run the MapReduce Job
Ensure you have Hadoop set up and running, then execute the following command:

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -files <path_to_local_mapper>/Mapper.py,<path_to_local_reducer>/Reducer.py -input /WordCount/input/words.txt -output /hcmus/<StudentID>/output -mapper "python3 Mapper.py" -reducer "python3 Reducer.py"

*Clean Output (If Rerun)
If you rerun the job, clear the old output directory first:

	hdfs dfs -rm -r /WordCount/output

4. View the ouput file

	hdfs dfs -cat /WordCount/output/part-00000

Example output:
a	28647
f	13399
j	3832
g	13061
h	16011
c	35664
m	23464
u	23225
s	42412

5. Retrieve the Results
After the job completes successfully, download the results to your local machine:

	hdfs dfs -get /WordCount/output/part-00000 results.txt