package src.country;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringAndInt implements WritableComparable<StringAndInt> {

	String tag;
	Integer occurrence;

	@Override
	public void readFields(DataInput arg0) throws IOException {
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	}

	@Override
	public int compareTo(StringAndInt arg0) {
		return 0;
	}

}