import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class PaysAndTags implements WritableComparable<PaysAndTags> {
	@Override
	public String toString() {
		return "PaysAndTags [pays=" + pays + ", tag=" + tag + "]";
	}

	String pays ;
	String tag;	

	public PaysAndTags() {
		super();
		tag = "";
		pays = "";
	}

	public PaysAndTags(String pays, String tag) {
		this.tag = tag;
		this.pays = pays;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Text t = new Text();
		t.readFields(in);
		pays = t.toString();
		t.readFields(in);
		tag = t.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text t = new Text(pays);
		t.write(out);
		t = new Text(tag);
		t.write(out);
	}

	@Override
	public int compareTo(PaysAndTags o) {
		int r =pays.compareTo(o.pays) ;
		return r==0?tag.compareTo(o.tag):r;
	}

}