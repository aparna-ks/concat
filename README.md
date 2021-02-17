# Common csv operations I have 

# Concatenate
In one of my projects, the input files were generated every few minutes and end of the day these files were concatentaed to be analyzed. Initially I used Pandas but using Python was faster. Each file has header row which is in the concatenated file.

# Convert to parquet
Input csv files were converted to parquet files as parquet files take less space and is faster. Included functions to read from s3 and save back to s3 or do the conversion locally. 


