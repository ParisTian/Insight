# Table of Contents
1. [Description of the task](README.md#description-of-the-task)
2. [Dependencies](README.md#dependencies)
3. [Solution](README.md#solution)
4. [Run instructions](README.md#run-instructions)
5. [Redesign for processing massive data](README.md#redesign-for-processing-massive-data)

# Description of the task

Please refer to the following link for the description of the design: [https://github.com/InsightDataScience/edgar-analytics](https://github.com/InsightDataScience/edgar-analytics)

# Dependencies

This implementation used opencsv-3.3.jar to parse csv file. This file is put under 'src' directory along with the sessionization.java. And the compiling and running command has included that file. Basically speaking, you don't need to do anything.

# Solution

* Parse cvs file line by line:
	* Put new record into a queue. This queue keeps all the records during the time slot: 'X - inact_period' to 'X'
	* Use a map to keep track the active IP, and its start time, latest time. 
		* if cur IP is in map, update latest time, and web counter++
		* else put cur IP in map, set start time, latest time, rank.
	* When X+1 time comes, check all the records occured at or before moment 'X - inact_period', get the latest time from the map.
		* if inactive for over 'inact_period', put this IP into file, remove from map.
	* When EOF
		* Put all the leftover into a heap to reorder in asending sequence, 
			* first check start time
			* if same start time, check the sequence of appearance. This information is record in IpNode.rank.
		* Then move the record one by one from heap to file, till it is empty.
		
* Error Handling:
	* Read input file inactivity_period.txt, check the validation.
		* if < 1 or > 86400, report WARNING. For case < 1, will use 1 as inactivity period.
	* Read input file log.csv, get the indices of the required fields, check the validation of the file.
		* get the indices for required fields: "ip", "date", "time" ...
		* if required fields are not there, report ERROR and EXIT
		* if required fields appears more than once, report WARNING, use the last one.
		
* Methods: 
	* `edgarAnalytics`  : Analyze given EDGAR file, output a summary into output file.
	* `getInactPeriod`  : Get inactive period from given file. Check the validation.
	* `openLogFile`     : open Log file and check the validation.
	* `openOutFile`     : Open outout file
	* `analyzeLogFile`  : Analyze log file line by line.
	* `reorderLeftOver` : Reorder the leftover according to requirment
	* `putToFile`       : Compose the message and put into file
	* `my_print_error`  : Print error message
	* `my_print_warning`: Print warning message

* Class:
	* `IpNode`          : Keep the start time, latest time, rank, web_counter.
	
# Run instructions

The run instruction is exactly the same as mentioned in the assignment

    insight_testsuite~$ ./run_tests.sh 

However, I added following lines in run.sh. You don't need to do anything about this.

	javac -cp "./src/opencsv-3.3.jar" ./src/sessionization.java
	java  -cp "./src;./src/opencsv-3.3.jar" sessionization

# Redesign for processing massive data

For processing massive data, we could split the source data into several data file, and process them seperatly.

If the memeoy is not big enough to put the queue, then we could use more pointers to point to the different position of the source file for processing. For example, the queue is for saving the records from time X with time slot T, then use fp1 points to X, fp2 to X+T, then load records from X to X+T/2, use fp3 to keep record of position of X+T/2.

For sorting, if memory is not big enough, then we need to apply external merge sort.

