package main.java.geldit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class CompositePartitioner implements 
Partitioner<CompositeWritable, IntWritable>{

	public int getPartition(CompositeWritable key,
			IntWritable value, int numPartitions) {
		return (key.getNkey().hashCode()  % numPartitions);
	}

	public void configure(JobConf conf) {

		
	}
}
