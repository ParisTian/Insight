# Table of Contents
1. [Description of the task](README.md#description-of-the-task)
2. [Dependencies](README.md#dependencies)
3. [Solution](README.md#solution)
4. [Run instructions](README.md#run-instructions)
5. [Redesign for processing massive data](README.md#redesign-for-processing-massive-data)

# Description of the task

Please refer to the following link for the description of the design: [https://github.com/InsightDataScience/edgar-analytics](https://github.com/InsightDataScience/edgar-analytics)

# Dependencies

This implementation used `opencsv-3.3.jar` to parse csv file. This file is put under `src` directory along with the `sessionization.java`. And the compiling and running command has included that file. Basically speaking, you don't need to do anything.

# Solution

* Parse cvs file line by line:
	* Put new record into a queue. This queue keeps all the records during the time slot: 'X - inact_period' to 'X'
	* Use a map (key=IP, value=IpNode) to keep track the active IP, and its start time, latest time. 
		* if cur IP is in map, update latest time, and web counter++
		* else put cur IP in map, set start time, latest time, rank.
	* When X+1 time comes, 
		* check all the records occured at or before moment 'X - inact_period'
			* cur IP's information is in IpNode, use IP as key, get it's information. Use the latest_time to calculate inactive time.
			* if inactive for over 'inact_period', put this IP into file, remove from map.
		* put new incoming record occured at X+1 into map. web page ++ 
			* if map has this IP, update latest accessing time
			* if map doesn't have this IP, setup a new record.
	* When EOF
		* Put all the leftover into a heap to reorder in asending sequence, 
			* first check start time
			* if same start time, check the sequence of appearance. This information is record in IpNode.rank.
		* Then move the record one by one from heap to file, till it is empty.
		
* Error Handling:
	* Read input file `inactivity_period.txt`, check the validation.
		* if `inact_period < 1` or `inact_period > 86400`, report WARNING. For case `inact_period < 1`, will use 1 instead.
	* Read input file `log.csv`, get the indices of the required fields, check the validation of the file.
		* get the indices for required fields: "ip", "date", "time" ...
		* if required fields are not there, report ERROR and EXIT
		* if required fields appears more than once, report WARNING, use the last one.
		
* Methods: 
	* `edgarAnalytics`  : Analyze given EDGAR file, output a summary into output file.
	* `getInactPeriod`  : Get inactive period from file `inactivity_period.txt`. Check the validation.
	* `openLogFile`     : open `log.csv` file and check the validation.
	* `openOutFile`     : Open output file `sessionization.txt`
	* `analyzeLogFile`  : Analyze log file line by line.
	* `reorderLeftOver` : Reorder the leftover according to requirment
	* `putToFile`       : Compose the message and put into file `sessionization.txt`
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

For processing massive data, we could use map-reduce strategy plus external merge sort. 

First, chop file into many chunks. Each chunk is deployed as one map task. These map tasks will generate <key, value> pairs, with IP address as key, the related informations as value. 

Second, each reduce task will work on only a collection of IP addresses, calculate the time and the count of web pages. Write the outcome an individual file. 

Third, now we have many sub-files, each of the sub-file has the outcome of a collection of IP addresses. Use external merge sort to merge these individual files into one output file.

