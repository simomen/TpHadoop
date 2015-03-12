package src.country;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryReducer extends Reducer<Text, Text, Text, Text> {

	Map<String, Integer> mapTag = new HashMap<String, Integer>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (Text value : values) {
			if (mapTag.containsKey(value.toString()))
				sum = mapTag.get(value.toString());
			sum++;
			mapTag.put(value.toString(), sum);
			
			if (sum == 4)
				context.write(key, new Text(value));
		}
	}
}
