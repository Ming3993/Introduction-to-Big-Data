# How to run source code
## Step 1: Change directory to the source code folder, e.g. 22120210/22120405/src/WordCount/WordCount
```bash
$ cd 22120210/22120405/src/WordCount/WordCount
```

## Step 2: Install Maven 
This tutorial guides through the installation on Ubuntu 22.04
```bash
$ sudo apt install maven -y
```

## Step 3: Build the the project
```bash
$ mvn install
```
The project jar file is now in target/ folder. The file name is WordCount-1.0-SNAPSHOT.jar.

## Step 4: Put the words.txt into HDFS
```bash
$ hdfs dfs -put <destination_folder>
```

## Step 5: Run MapReduce job
```bash
$ hadoop jar target/WordCount-1.0-SNAPSHOT.jar org.hcmus.bigdata.nttuan.WordCountDriver <input_path> <output_path>
```
In this case, <input_path> is where the words.txt file is stored. <output_path> is where the result folder is stored.

## Step 6: Check the result
```bash
$ hdfs dfs -cat <output_path>/result/part-r-00000
```