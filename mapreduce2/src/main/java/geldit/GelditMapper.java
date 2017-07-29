package main.java.geldit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GelditMapper extends Mapper<LongWritable, Text, CompositeWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable mkey, Text mvalue, Context context)
			throws IOException, InterruptedException{
		String[] tokens = mvalue.toString().split("\t");
		String date = tokens[1];
		String country = tokens[7];
		if(! country.isEmpty() && country != null){
			CompositeWritable cw = new CompositeWritable(country, date);
			context.write(cw, one);
		}
	}
}
