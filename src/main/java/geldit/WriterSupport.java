package main.java.geldit;

import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class WriterSupport extends WriteSupport<Group> {

	public static final String PARQUET_EXAMPLE_SCHEMA = "parquet.example.schema";

	public static void setSchema(MessageType schema, Configuration configuration) {
		configuration.set(PARQUET_EXAMPLE_SCHEMA, schema.toString());
	}

	public static MessageType getSchema(Configuration configuration) {
		return parseMessageType(checkNotNull(configuration.get(PARQUET_EXAMPLE_SCHEMA), PARQUET_EXAMPLE_SCHEMA));
	}

	private MessageType schema;
	private GroupWriter groupWriter;

	@Override
	public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(Configuration configuration) {
		schema = getSchema(configuration);
		return new WriteContext(schema, new HashMap<String, String>());
	}

	@Override
	public void prepareForWrite(RecordConsumer recordConsumer) {
		groupWriter = new GroupWriter(recordConsumer, schema);
	}

	@Override
	public void write(Group record) {
		groupWriter.write(record);
	}

}
