import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class CopyOfStringAndInt implements Comparable<CopyOfStringAndInt> , Writable {
	Integer number;
	Text tag;

	public CopyOfStringAndInt() {
		super();
		tag = new Text("");
		number = 0;
	}

	public CopyOfStringAndInt(Text tag, Integer i) {
		this.tag = tag;
		this.number = i;
	}
	
	public CopyOfStringAndInt(String tag, IntWritable i) {
		this.tag = new Text(tag);
		this.number = i.get();
	}

	@Override
	public int compareTo(CopyOfStringAndInt o) {
		return -this.number.compareTo(o.number);
	}

	@Override
	public String toString() {
		return "StringAndInt [number=" + number + ", tag=" + tag + "]";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		IntWritable iw =  new IntWritable();
		iw.readFields(arg0);
		tag.readFields(arg0);
		number = iw.get();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		IntWritable iw = new IntWritable(number);
		iw.write(arg0);
		tag.write(arg0);
	}

}