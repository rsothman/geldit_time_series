package main.java.geldit;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


public class GelditAnalyticParquet extends Configured implements Tool{

	private static final Schema SCHEMA = new Schema.Parser().parse(
			"{\n" +
					"  \"type\": \"record\",\n" +
					"  \"name\": \"Line\",\n" +
					"  \"fields\": [\n" +
					"    {\"name\": \"country\", \"type\": \"string\"},\n" +
					"    {\"name\": \"date\", \"type\": \"string\"},\n" +
					"    {\"name\": \"count\", \"type\": \"long\"}\n" +
					"  ]\n" +
			"}");

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputpath = args[0];
		String outputpath = args[1];
		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf);
		job.setJarByClass(GelditAnalytic.class);

		job.setMapperClass(GelditMapper.class);
		job.setReducerClass(GelditParquetReducer.class);
		job.setCombinerClass(GelditCompiner.class);

		job.setMapOutputKeyClass(CompositeWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(CompositeComparator.class);
		job.setGroupingComparatorClass(CompositeComparator.class);
		job.setPartitionerClass(CompositePartitioner.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setSchema(job, SCHEMA);
		AvroParquetOutputFormat.setOutputPath(job, new Path(outputpath));
		AvroParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
		FileInputFormat.addInputPath(job, new Path(inputpath));

		if(fs.exists(new Path(outputpath)))
		{
			fs.delete(new Path(outputpath), true);
		}

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new GelditAnalyticParquet(), args);
		System.exit(exitCode);
	}

}
