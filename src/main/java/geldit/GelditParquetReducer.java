package main.java.geldit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/* Parquet Reducer is running. but still under testing and more development */

public class GelditParquetReducer extends 
	Reducer<CompositeWritable, IntWritable, Void, GenericRecord>{
	private Map <String,ParquetWriter> writers;
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
	public void setup(Context context) throws IOException, InterruptedException{
		writers = new HashMap<String,ParquetWriter>();
	}
	public void reduce(CompositeWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		String outdir = conf.get("mapred.output.dir");
		context.getCounter("Reduce Parquet Record", "ProcessedRecord").increment(1);
		ParquetWriter writer;
		if(writers.containsKey(key.getNkey().toString())){
			writer = writers.get(key.getNkey().toString());
		}else{
			/*
			 AvroParquetOutputFormat<GenericRecord> apwriter = 
					new AvroParquetOutputFormat<GenericRecord>();
			*/
			Path outpath = new Path(outdir + 
					"/country="+key.getNkey().toString()+"/part-" + 
					context.getTaskAttemptID().getId() + ".parquet");
			/*
			  writer = apwriter.getRecordWriter(context.getConfiguration(), outpath,
					CodecConfig.from(context).getCodec());
			*/

                        writer = AvroParquetWriter.builder(outpath)
                        		.withCompressionCodec(CompressionCodecName.GZIP)
                        		.withSchema(SCHEMA).build();
                        writers.putIfAbsent(key.getNkey().toString(), writer);
		}
		int sum = 0;
		for (IntWritable value: values){
			sum += value.get();
		}
		GenericRecord record = new GenericData.Record(SCHEMA);
		record.put("country", key.getNkey());
		record.put("date", key.getNValue());
		record.put("count", sum);
		writer.write(record);
		context.getCounter("Reduce Parquet Record", "WrittenRecord").increment(1);

	}
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for (Map.Entry<String,ParquetWriter> entry : writers.entrySet()){
			entry.getValue().close();
		}
	}
}