# How to run the code
## Datasets setting up and environment preparation
### Step 1: Create and activate your virtual environment
```bash
python3 -m venv your_venv
```
Example command:
```bash
python3 -m venv lab2
```
Activate your virtual environment
```bash
source ~/your_venv/bin/activate
```
Example command:
```bash
source ~/lab2/bin/activate
```
Then, change your working directory to the source code folder.
```bash
cd 22120210
```
Run the following command to install essential library:
```bash
pip install -r requirements.txt
```
### Step 2: Upload the input files to HDFS
Create a directory on HDFS to store your input files.
```bash
hdfs dfs -mkdir -p /hcmus/lab2
```
Upload files to HDFS.
```bash
hdfs dfs -put Datasets/{asr.csv,shapes.parquet} /hcmus/lab2
```
## Task 2.1: Run your MapReduce job 
### Method 1: With a Python interpreter on local machine
```bash
python src/Task_2.1/22120210.py Datasets/asr.csv > src/Task_2.1/output.csv
```
Expected folder structure:
```
22120210/
├── ...
├── src/
│   ├── Task_2.1/
│   |   ├── 22120210.py
│   |   └── output.csv
│   ├── Task_2.2/
|   └── Task_2.3/
├── ...
└── requirements.txt
```

## Task 2.2: Run your Spark job with a Python interpreter on local machine
```bash
python src/Task_2.2/22120210.py
```
The `output.csv` file will be automatically exported in the directory `src/Task_2.2`. Expeceted folder structure:
```
22120210/
├── ...
├── src/
│   ├── Task_2.1/
│   ├── Task_2.2/
│   |   ├── 22120210.py
│   |   └── output.csv
|   └── Task_2.3/
├── ...
└── requirements.txt
```
## Task 2.3: Submit a Spark job to run on distributed environment
```bash
spark-submit src/Task_2.3/22120210.py
```
The output file will be stored in `/hcmus/lab2/results`, to save
as a single `output.csv` file, run the command:
```bash
hdfs dfs -getmerge /hcmus/lab2/results src/Task_2.3/output.csv
```
Expected folder structure:
```
22120210/
├── ...
├── src/
│   ├── Task_2.1/
│   ├── Task_2.2/
|   └── Task_2.3/
│       ├── 22120210.py
│       └── output.csv
├── ...
└── requirements.txt
```