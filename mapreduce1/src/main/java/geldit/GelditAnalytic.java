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
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;


public class GelditAnalytic {

	public static void main(String[] args) throws Exception
	{
		MessageType schema = MessageTypeParser.parseMessageType(
				"message group {\n" +
						" required binary country (UTF8);\n" +
						" required binary date (UTF8);\n" +
						" required binary count (UTF8);\n" +
				"}");
		Configuration conf = new Configuration();
		String inputpath = args[0];
		String outputpath = args[1];
		FileSystem fs = FileSystem.get(conf);
		JobConf jobconf = new JobConf(GelditAnalytic.class); 
		//Job jobctl = new Job(jobconf);
		jobconf.setJobName("GelditAnalyticParquet");
		jobconf.setMapperClass(GelditMapper.class);
		jobconf.setCombinerClass(GelditCompiner.class);
		jobconf.setMapOutputKeyClass(CompositeWritable.class);
		jobconf.setMapOutputValueClass(IntWritable.class);
		jobconf.setPartitionerClass(CompositePartitioner.class);
		FileInputFormat.addInputPath(jobconf, new Path(inputpath));
		jobconf.setReducerClass(GelditParquetReducer.class);
		jobconf.setOutputFormat(DeprecatedParquetOutputFormat.class);
		DeprecatedParquetOutputFormat.setCompression(conf, CompressionCodecName.GZIP);
		DeprecatedParquetOutputFormat.setOutputPath(jobconf, new Path(outputpath));
		DeprecatedParquetOutputFormat.setAsOutputFormat(jobconf);
		DeprecatedParquetOutputFormat.setWriteSupportClass(jobconf, GroupWriteSupport.class);
		jobconf.set("parquet.example.schema", schema.toString());
		DeprecatedParquetOutputFormat.setOutputCompressorClass(jobconf, GzipCodec.class);

		if(fs.exists(new Path(outputpath)))
		{
			fs.delete(new Path(outputpath), true);
		}
		JobClient.runJob(jobconf);



	}

}