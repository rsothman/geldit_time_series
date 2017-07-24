package main.java.geldit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class TestParquet {
	public static void main(String args[]) throws IOException{
		MessageType schema = MessageTypeParser.parseMessageType(
				"message Pair {\n" +
				" required binary left (UTF8);\n" +
				" required binary right (UTF8);\n" +
				"}");
		GroupFactory groupFactory = new SimpleGroupFactory(schema);
		Group group = groupFactory.newGroup()
		.append("left", "L")
		.append("right", "R");
		Configuration conf = new Configuration();
		System.out.println("The conf is "+conf);
		Path path = new Path("data.parquet");
		GroupWriteSupport writeSupport = new GroupWriteSupport();
		conf.set("parquet.write.support.class", writeSupport.getClass().getName());
		GroupWriteSupport.setSchema(schema, conf);
		ParquetWriter<Group> writer = new ParquetWriter<Group>(path, writeSupport,
		ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
		ParquetWriter.DEFAULT_BLOCK_SIZE,
		ParquetWriter.DEFAULT_PAGE_SIZE,
		ParquetWriter.DEFAULT_PAGE_SIZE, /* dictionary page size */
		ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
		ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
		ParquetProperties.WriterVersion.PARQUET_1_0, conf);
		System.out.println(conf.get("parquet.write.support.class"));
		try{
			System.out.print("the scema in conf is "+conf.getClassByName(conf.get("org.apache.parquet.example.schema")).newInstance());
		}catch(Exception e){
			
		}
		writer.write(group);
		writer.close();
	}

}
