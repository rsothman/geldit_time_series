package main.java.geldit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class GelditMapper extends MapReduceBase implements Mapper<LongWritable, Text, CompositeWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable mkey, Text mvalue, OutputCollector<CompositeWritable, IntWritable> output, Reporter reporter)
			throws IOException{
		String[] tokens = mvalue.toString().split("\t");
		String date = tokens[1];
		String country = tokens[7];
		if(! country.isEmpty() && country != null){
			CompositeWritable cw = new CompositeWritable(country, date);
			output.collect(cw, one);
		}
	}

}
