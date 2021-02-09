# concat csv files in a directory
In one of my projects, the input files were generated every few minutes and end of the day these files were concatentaed to be analyzed. Initially I used Pandas but using Python files was faster. 

Another good solution was sed 1d *.csv > output.csv

