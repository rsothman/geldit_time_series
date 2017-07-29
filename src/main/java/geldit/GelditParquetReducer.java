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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.avro.Schema;

public class GelditParquetReducer extends MapReduceBase
implements Reducer<CompositeWritable, IntWritable, Void, Group>{

	private Map <String, RecordWriter> writers;
	private String outdir;
	private String tip_id;
	private JobConf conf;
	Reporter reporterref;
	MessageType schema = MessageTypeParser.parseMessageType(
			"message group {\n" +
					" required binary country (UTF8);\n" +
					" required binary date (UTF8);\n" +
					" required binary count (UTF8);\n" +
			"}");


	public void configure(JobConf job) {
		conf = job;
		writers = new HashMap<String,RecordWriter>();
		outdir = conf.get("mapred.output.dir");
		tip_id = conf.get("mapred.tip.id");
	}

	public void reduce(CompositeWritable key, Iterator<IntWritable> values,
			OutputCollector<Void, Group> output,
			Reporter reporter) throws IOException{




		reporterref = reporter;
		RecordWriter writer;
		if(writers.containsKey(key.getNkey().toString())){
			writer = writers.get(key.getNkey().toString());
		}else{
			FileSystem fs = FileSystem.get(conf);
			DeprecatedParquetOutputFormat<Group> apwriter = 
					(DeprecatedParquetOutputFormat<Group>) conf.getOutputFormat();

			writer = apwriter.getRecordWriter(fs, conf, "country="+key.getNkey().toString()+
					"/part-" + tip_id.substring(tip_id.length()-10)
					, new Progressable() {
				public void progress() {
					System.out.println("Doing Progress");
				} 
			});

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
				.append("count", String.valueOf(sum));

		writer.write(null, group);

	}
	public void close() throws IOException{
		for (Map.Entry<String,RecordWriter> entry : writers.entrySet()){
			entry.getValue().close(reporterref);
		}
	}
}