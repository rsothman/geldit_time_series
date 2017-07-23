package main.java.geldit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


public class GelditAnalytic extends Configured implements Tool{
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
		String outputtype = args[2];
		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf);
		job.setJarByClass(GelditAnalytic.class);

		job.setMapperClass(GelditMapper.class);
		job.setCombinerClass(GelditCompiner.class);

		job.setMapOutputKeyClass(CompositeWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(CompositeComparator.class);
		job.setGroupingComparatorClass(CompositeComparator.class);
		job.setPartitionerClass(CompositePartitioner.class);

		job.setOutputKeyClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));

		if(outputtype.equals("parquet")){
			job.setReducerClass(GelditParquetReducer.class);
			job.setOutputFormatClass(AvroParquetOutputFormat.class);
			job.setOutputValueClass(GenericRecord.class);
			AvroParquetOutputFormat.setSchema(job, SCHEMA);
			AvroParquetOutputFormat.setOutputPath(job, new Path(outputpath));
			AvroParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
		}else if(outputtype.equals("text")){
			job.setReducerClass(GelditReducer.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(outputpath));
		}
		else {
			System.err.println("Unsupported output format" + outputtype);
			System.exit(5);
		}

		if(fs.exists(new Path(outputpath)))
		{
			fs.delete(new Path(outputpath), true);
		}

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		Schema SCHEMA = new Schema.Parser().parse(
				"{\n" +
						"  \"type\": \"record\",\n" +
						"  \"name\": \"Line\",\n" +
						"  \"fields\": [\n" +
						"    {\"name\": \"country\", \"type\": \"string\"},\n" +
						"    {\"name\": \"date\", \"type\": \"string\"},\n" +
						"    {\"name\": \"count\", \"type\": \"long\"}\n" +
						"  ]\n" +
				"}");
		Configuration conf = new Configuration();
		String inputpath = args[0];
		String outputpath = args[1];
		String outputtype = args[2];
		FileSystem fs = FileSystem.get(conf);
		Job jobctl = new Job(conf);
		JobConf job = new JobConf(GelditAnalytic.class); 
		job.setJobName("GelditAnalyticParquet");
		job.setMapperClass(GelditMapper.class);
		job.setCombinerClass(GelditCompiner.class);
		job.setMapOutputKeyClass(CompositeWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(CompositePartitioner.class);
		job.setOutputKeyClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));

		if(outputtype.equals("parquet")){
			job.setReducerClass(GelditParquetReducer.class);
			job.setOutputFormat(DeprecatedParquetOutputFormat.class);
			job.setOutputValueClass(GenericRecord.class);
			//AvroParquetOutputFormat.setSchema(job, SCHEMA);
			//AvroParquetOutputFormat.setOutputPath(jobctl, new Path(outputpath));
			DeprecatedParquetOutputFormat.setCompression(jobctl, CompressionCodecName.GZIP);
			DeprecatedParquetOutputFormat.setOutputPath(jobctl, outputpath);
			DeprecatedParquetOutputFormat.setAsOutputFormat(conf);
		}else if(outputtype.equals("text")){
			job.setReducerClass(GelditReducer.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(outputpath));
		}
		else {
			System.err.println("Unsupported output format" + outputtype);
			System.exit(5);
		}

		if(fs.exists(new Path(outputpath)))
		{
			fs.delete(new Path(outputpath), true);
		}


		
	}

}