package main.java.geldit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GelditCompiner extends 
	Reducer<CompositeWritable, IntWritable, CompositeWritable, IntWritable>{
	public void reduce(CompositeWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
		int sum = 0;
		for (IntWritable value: values){
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
