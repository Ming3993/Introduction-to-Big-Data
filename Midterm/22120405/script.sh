#!/bin/bash
# This script runs a Hadoop job for a specified problem and testcase.

# Check if at least 2 arguments are provided
if [ $# -lt 2 ]; then
    echo "Usage: <script.sh> <problem_number> <testcase_number> [is-update=<is_update_file>] [extra-arg=extra_args] [is-build=<is_build>]"
    echo "Example: <script.sh> 1 1 [is-update=true] [extra-arg=arg1] [is-build=true]"
    exit 1
fi

# Determine the problem path based on the problem number
problem=$1
if [ $problem -eq 1 ]; then
    problem_path="Level1-Question5"
elif [ $problem -eq 2 ]; then
    problem_path="Level1-Question6"
elif [ $problem -eq 3 ]; then
    problem_path="Level2-Question11"
fi

# Check if the problem directory exists
if [ ! -d "$problem_path" ]; then
    echo "Problem directory $problem_path does not exist."
    exit 1
fi

# Check if the specific testcase directory exists
testcase_num=$2
testcase_path="${problem_path}/testcase/testcase${testcase_num}"
if [ ! -d "$testcase_path" ]; then
    echo "Testcase directory $testcase_path does not exist."
    exit 1
fi

# Construct path to the built JAR file
is_build=false
for arg in "${@:3}"; do
    if [[ $arg == is-build=* ]]; then
        is_build="${arg#is-build=}"
    fi
done

src_path="${problem_path}/src"
jar_path="${problem_path}/src/target/${problem_path}-1.0-SNAPSHOT.jar"

echo "Checking for JAR file at $jar_path"
if [[ ! -f "$jar_path" || "$is_build" == true ]]; then
    echo "Building the JAR file."
    mvn clean install -f "$src_path"
fi
echo "Found JAR file at $jar_path"
echo

# Determine the driver class name
driver_class="com.hcmus.tuannt.$(echo "$problem_path" | tr '-' '_')_Driver"

# Print basic info
echo "Problem: $problem_path"
echo "Testcase: $testcase_num"
echo "Jar file: $jar_path"
echo "Driver class: $driver_class"
echo

# Define HDFS input path
hdfs_input_path="/${problem_path}/testcase${testcase_num}/input"

# Parse is-update flag (defaults to false)
is_update_file=false
for arg in "${@:3}"; do
    if [[ $arg == is-update=* ]]; then
        is_update_file="${arg#is-update=}"
    fi
done

# If is-update is true, remove existing HDFS input directory
if [ "$is_update_file" = true ]; then
    hdfs dfs -rm -r "$hdfs_input_path"
fi

# If HDFS input directory doesn't exist, create and upload files
if ! hdfs dfs -test -d "$hdfs_input_path"; then
    echo "Creating directory ${hdfs_input_path} in HDFS."
    hdfs dfs -mkdir -p "$hdfs_input_path"
    
    echo "Uploading files from ${testcase_path} to HDFS at ${hdfs_input_path}."
    for file in "${testcase_path}"/*; do
        if [ -f "$file" ]; then
            echo "Uploading $file to HDFS."
            hdfs dfs -put "$file" "$hdfs_input_path"
        else
            echo "Skipping $file as it is not a regular file."
            continue
        fi
    done
else
    echo "Directory ${hdfs_input_path} already exists in HDFS."
fi
echo

# List input files in HDFS
echo "Input files in HDFS:"
hdfs dfs -ls "$hdfs_input_path"
echo

# Define output path and remove it if it already exists
hdfs_output_path="${hdfs_input_path}/../output"
if hdfs dfs -test -d "$hdfs_output_path"; then
    echo "Removing existing output directory ${hdfs_output_path} in HDFS."
    hdfs dfs -rm -r "$hdfs_output_path"
fi

# Run Hadoop job with optional extra argument
echo "Running Hadoop job."
extra_arg=""
for arg in "${@:3}"; do
    if [[ $arg == extra-arg=* ]]; then
        extra_arg="${arg#extra-arg=}"
    fi
done

if [ -n "$extra_arg" ]; then
    echo "An extra argument found: $extra_arg"
    hadoop jar "$jar_path" "$driver_class" "$hdfs_input_path" "$hdfs_input_path/../output" "$extra_arg"
else
    hadoop jar "$jar_path" "$driver_class" "$hdfs_input_path" "$hdfs_input_path/../output"
fi
echo

# Show output files in HDFS
echo "Output files in HDFS:"
hdfs dfs -ls "$hdfs_output_path"
echo

# Download output from HDFS to local filesystem
host_output_path="${testcase_path}/output"
echo "Downloading output files from HDFS to local directory ${host_output_path}."
if [ -d "${host_output_path}" ]; then
    echo "Output directory already exists. Removing it."
    rm -rf "${host_output_path}"
fi
mkdir -p "${host_output_path}"
hdfs dfs -get "$hdfs_output_path"/* "${host_output_path}"
if [ $? -ne 0 ]; then
    echo "Failed to download output files from HDFS."
    exit 1
fi
echo

echo "Output files downloaded successfully to ${host_output_path}."
echo "Script execution completed successfully."