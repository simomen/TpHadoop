import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class StringAndInt implements WritableComparable<StringAndInt> {
	Integer number;
	String tag;

	public StringAndInt() {
		super();
		tag = new String("");
		number = 0;
	}
	
	public StringAndInt(String tag, Integer i) {
		this.tag = tag;
		this.number = i;
	}

	public StringAndInt(Text tag, Integer i) {
		this.tag = tag.toString();
		this.number = i;
	}
	
	public StringAndInt(String tag, IntWritable i) {
		this.tag = tag;
		this.number = i.get();
	}
	
	public StringAndInt(Text tag, IntWritable i) {
		this.tag = tag.toString();
		this.number = i.get();
	}

	@Override
	public int compareTo(StringAndInt o) {
		return -this.number.compareTo(o.number);
	}

	@Override
	public String toString() {
		return "StringAndInt [number=" + number + ", tag=" + tag + "]";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {

		tag = arg0.readUTF();
		number = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(tag);
		arg0.writeInt(number);
	}

}