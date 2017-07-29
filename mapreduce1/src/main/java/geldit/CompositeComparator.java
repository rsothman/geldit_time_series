package main.java.geldit;

import org.apache.hadoop.io.WritableComparator;

public class CompositeComparator extends WritableComparator {
	public CompositeComparator() {
		super(CompositeWritable.class, true);
	}
	public int compare(CompositeWritable c1, CompositeWritable c2) {
		CompositeWritable cw1 = (CompositeWritable) c1;
		CompositeWritable cw2 = (CompositeWritable) c2;
		return cw1.getNkey().compareTo(cw2.getNkey());
	}
}
