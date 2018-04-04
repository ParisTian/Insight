#!/bin/bash
#
# Use this shell script to compile (if necessary) your code and then execute it. Below is an example of what might be found in this file if your program was written in Python
#
#python ./src/sessionization.py ./input/log.csv ./input/inactivity_period.txt ./output/sessionization.txt

javac -cp "./src/opencsv-3.3.jar" ./src/sessionization.java
java  -cp "./src;./src/opencsv-3.3.jar" sessionization
