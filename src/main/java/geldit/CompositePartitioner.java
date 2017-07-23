package main.java.geldit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CompositePartitioner extends 
Partitioner<CompositeWritable, IntWritable>{

	@Override
	public int getPartition(CompositeWritable key,
			IntWritable value, int numPartitions) {
		return (key.getNkey().hashCode()  % numPartitions);
	}
}
