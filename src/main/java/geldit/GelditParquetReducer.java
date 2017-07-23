package main.java.geldit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/* Parquet Reducer is running. but still under testing and more development */

public class GelditParquetReducer extends MapReduceBase
implements Reducer<CompositeWritable, IntWritable, Void, GenericRecord>{

	private Map <String, RecordWriter> writers;
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
	private JobConf conf;
	Reporter reporterref;

	public void configure(JobConf job) {
		conf = job;
		writers = new HashMap<String,RecordWriter>();
	}
	/*
 	public void setup(Context context) throws IOException, InterruptedException{


	}
	 */

	public void reduce(CompositeWritable key, Iterator<IntWritable> values,
			OutputCollector<Void, GenericRecord> output,
			Reporter reporter) throws IOException{


		String outdir = conf.get("mapred.output.dir");
		reporterref = reporter;
		RecordWriter writer;
		if(writers.containsKey(key.getNkey().toString())){
			writer = writers.get(key.getNkey().toString());
		}else{
			FileSystem fs = FileSystem.get(conf);
			DeprecatedParquetOutputFormat<GenericRecord> apwriter = 
					new DeprecatedParquetOutputFormat<GenericRecord>();
			Path outpath = new Path(outdir + 
					"/country="+key.getNkey().toString()+"/part-" + 
					conf.get("mapred.task.id") + ".parquet");
			
			  writer = apwriter.getRecordWriter(fs, conf, "country="+key.getNkey().toString()+
					  "/part-" + conf.get("mapred.task.id"), 
					  new Progressable() {
					        public void progress() {
					        	System.out.println("Doing Progress");
					        	} 
					        });
			/*

			writer = (RecordWriter) DeprecatedParquetOutputFormat.builder(outpath)
					.withCompressionCodec(CompressionCodecName.GZIP)
					.withSchema(SCHEMA).build();
			*/
			writers.put(key.getNkey().toString(), writer);
		}
		int sum = 0;
		while (values.hasNext()){
			sum += values.next().get();
		}
		GenericRecord record = new GenericData.Record(SCHEMA);
		record.put("country", key.getNkey());
		record.put("date", key.getNValue());
		record.put("count", sum);
		writer.write(null, record);

	}
	public void close() throws IOException{
		for (Map.Entry<String,RecordWriter> entry : writers.entrySet()){
			entry.getValue().close(reporterref);
		}
	}
}