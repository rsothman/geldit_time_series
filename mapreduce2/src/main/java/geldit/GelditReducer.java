package main.java.geldit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class GelditReducer extends Reducer<CompositeWritable, IntWritable, NullWritable, Text>{
	private MultipleOutputs<NullWritable, Text> multioutput;

	public void setup(Context context) throws IOException, InterruptedException{
		multioutput = new MultipleOutputs<NullWritable, Text>(context);
	}
	public void reduce(CompositeWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int sum = 0;
		for (IntWritable value: values){
			sum += value.get();
		}
		Text output = new Text(key.getNkey().toString() +"\t"+key.getNValue().toString() +"\t"+Integer.toString(sum));

		multioutput.write(NullWritable.get(), output, "country="+key.getNkey().toString()+"/");
	}
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		multioutput.close();
	}
}
