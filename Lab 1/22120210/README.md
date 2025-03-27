# How to run the Hadoop Streaming Job
## Step 1: Local testing before running with Hadoop Streaming (Optional)
```bash
$ cat /mnt/win10_linked/words.txt | python3 /mnt/win10_linked/mapper.py | python3 /mnt/win10_linked/reducer.py
```
## Step 2: Create a directory on HDFS to store resources of this lab
```bash
$ hdfs dfs -mkdir /word_count
```
## Step 3: Upload the input file on HDFS
```bash
$ hdfs dfs -put /mnt/win10_linked/words.txt /word_count
```
Verify the upload
```bash
$ hdfs dfs -ls /word_count
```
Example output:
```bash
Found 2 items
-rw-r--r--   1 khtn_22120210 supergroup        404 2025-03-25 04:56 /word_count/script.py
-rw-r--r--   1 khtn_22120210 supergroup    4862985 2025-03-26 14:31 /word_count/words.txt
```
## Step 4: Locate the Hadoop Streaming JAR file:
```bash
$ find / -name "hadoop-streaming*.jar" 2>/dev/null
```
Example output:
```bash
/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
/usr/local/hadoop/share/hadoop/tools/sources/hadoop-streaming-3.3.6-sources.jar
/usr/local/hadoop/share/hadoop/tools/sources/hadoop-streaming-3.3.6-test-sources.jar
```
## Step 5: Run the Hadoop Streaming Job
```bash
$ hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -input /word_count/words.txt -output /word_count/output -mapper "python3 /mnt/win10_linked/mapper.py" -reducer "python3 /mnt/win10_linked/reducer.py"
```
## Step 6: View the output file:
```bash
$ hdfs dfs -cat /word_count/output/part-00000
```
Example output:
```bash
a       28647
f       13399
j       3832
g       13061
h       16011
c       35664
m       23464
u       23225
s       42412
```
Save as result.txt file:
```bash
$ hdfs dfs -cat /word_count/output/part-00000 >> /mnt/win10_linked/results.txt
```
## References:

[Tutorial on local testing and Hadoop streaming](https://www.geeksforgeeks.org/hadoop-streaming-using-python-word-count-problem/)

