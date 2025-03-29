## Step 1: Create folder to store source code.
```bash
mkdir BigData
mkdir BigData/Lab
mkdir BigData/Lab/EX1
```

## Step 2: Open terminal in the folder store the code mapper and reducer.
```bash
cd ~/BigData/Lab/EX1
```

## Step 3: Create folder to store on HDFS.
```bash
hdfs dfs -mkdir /hcmus/Lab01/input
```

## Step 3: Upload the input file onto HDFS.
```bash
hdfs dfs -put ~/Downloads/words.txt /hcmus/Lab01/input
```

## Step 3: Run the Hadoop Streaming Job.
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -input /hcmus/Lab01/input/words.txt \
    -output /hcmus/Lab01/output \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file mapper.py \
    -file reducer.py
```

## Step 4: Check the output.
```
hdfs dfs -cat /hcmus/Lab01/output/*
```

## Step 5: Download the output as txt file.
```
hdfs dfs -get /hcmus/Lab01/output/part-00000 ~/BigData/Lab/EX1/result.txt
```
