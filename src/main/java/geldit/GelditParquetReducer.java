package main.java.geldit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/* Parquet Reducer is running. but still under testing and more development */

public class GelditParquetReducer extends MapReduceBase
implements Reducer<CompositeWritable, IntWritable, Void, Group>{

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
	MessageType schema = MessageTypeParser.parseMessageType(
			"message group {\n" +
					" required binary country (UTF8);\n" +
					" required binary date (UTF8);\n" +
					" required int64 count;\n" +
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
			OutputCollector<Void, Group> output,
			Reporter reporter) throws IOException{


		String outdir = conf.get("mapred.output.dir");

		reporterref = reporter;
		RecordWriter writer;
		if(writers.containsKey(key.getNkey().toString())){
			writer = writers.get(key.getNkey().toString());
		}else{
			/*Configuration config = new Configuration();
			GroupWriteSupport w = new GroupWriteSupport();
			w.setSchema(schema, conf);
			config.set("parquet.write.support.class", w.getClass().getName());
			System.out.println("The value for parquet.write.support.class is " + config.get("org.apache.parquet.write.support.class"));
			 */
			FileSystem fs = FileSystem.get(conf);
			DeprecatedParquetOutputFormat<Group> apwriter = 
					(DeprecatedParquetOutputFormat<Group>) conf.getOutputFormat();
			Path outpath = new Path(outdir + 
					"/country="+key.getNkey().toString()+"/part-" + 
					conf.get("mapred.tip.id") + ".parquet");

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
		GroupFactory groupFactory = new SimpleGroupFactory(schema);
		Group group = groupFactory.newGroup()
				.append("country", key.getNkey().toString())
				.append("date", key.getNValue().toString())
				.append("count", sum);
		/*
		GenericRecord record = new GenericData.Record(SCHEMA);
		record.put("country", key.getNkey());
		record.put("date", key.getNValue());
		record.put("count", sum);
		 */
		writer.write(null, group);

	}
	public void close() throws IOException{
		for (Map.Entry<String,RecordWriter> entry : writers.entrySet()){
			entry.getValue().close(reporterref);
		}
	}
}