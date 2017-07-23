package main.java.geldit;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class GelditCompiner extends MapReduceBase
implements Reducer<CompositeWritable, IntWritable, CompositeWritable, IntWritable>{

	public void reduce(CompositeWritable key, Iterator<IntWritable> values,
			OutputCollector<CompositeWritable, IntWritable> output, Reporter reporter)
					throws IOException{
		int sum = 0;
		while (values.hasNext()){
			sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	}
}
