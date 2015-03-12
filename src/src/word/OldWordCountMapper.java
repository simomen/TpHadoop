package src.word;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class OldWordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public Map<String, Integer> mapTampon;
	public Counter counter;

	public enum EnumCount {
		EMPTY_COUNTER
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		for (String word : value.toString().split("\\s+")) {
			// context.write(new Text(word), new IntWritable(1));

			if(word.equals(""))
				counter.increment(1);
			
			Integer count = 0;		
			if (mapTampon.containsKey(word))
				count = mapTampon.get(word);
			
			count++;
			mapTampon.put(word, count);
		}
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Iterator<String> it = mapTampon.keySet().iterator();
		while (it.hasNext()) {
			String string = (String) it.next();
			context.write(new Text(string),
					new IntWritable(mapTampon.get(string)));
		}
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		mapTampon = new HashMap<String, Integer>();
		counter = context.getCounter(EnumCount.EMPTY_COUNTER);
	}

}