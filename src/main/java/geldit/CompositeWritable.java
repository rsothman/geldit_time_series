package main.java.geldit;

import java.io.*;
import org.apache.hadoop.io.*;


;
public class CompositeWritable implements WritableComparable<CompositeWritable> {
	private Text naturalKey;
	private Text naturalValue;

	public CompositeWritable(){
		set(new Text(), new Text());
	}

	public CompositeWritable(String nkey, String nvalue){
		set(new Text(nkey), new Text(nvalue));
	}

	public CompositeWritable(Text nkey, Text nvalue){
		set(nkey, nvalue);
	}

	private void set(Text nkey, Text nvalue) {
		this.naturalKey = nkey;
		this.naturalValue = nvalue;
	}

	public Text getNkey(){
		return this.naturalKey;
	}

	public Text getNValue(){
		return this.naturalValue;
	}
	public void readFields(DataInput in) throws IOException {
		naturalKey.readFields(in);
		naturalValue.readFields(in);
	}

	@Override
	public int hashCode() {
		return naturalKey.hashCode() * 163 + naturalValue.hashCode();
	}

	public void write(DataOutput out) throws IOException {
		naturalKey.write(out);
		naturalValue.write(out);
	}

	public int compareTo(CompositeWritable cw) {
		int cmp = naturalKey.compareTo(cw.naturalKey);
		if(cmp != 0){
			return cmp;
		}
		return naturalValue.compareTo(cw.naturalValue);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof CompositeWritable) {
			CompositeWritable cw = (CompositeWritable) o;
			return naturalKey.equals(cw.naturalKey) && naturalValue.equals(cw.naturalValue);
		}
		return false;
	}

	@Override
	public String toString() {
		return naturalKey + "\t" + naturalValue;
	}


}
