import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Question2_2 {

	public static enum MyCounter {
		BlankLine
	}	

	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {

		private HashMap<String, Integer> hashMap;
		private Counter blankLineCounter;

		// private HashMap<String, MinMaxPriorityQueue<StringAndInt>>
		// CountriesTags;

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (String line : value.toString().split("\\n+")) {
				String[] words = line.split("\\t");
				String[] tags = words[8].split(",");
				Double longitude = Double.valueOf(words[10]); // longitude
				Double latitude = Double.valueOf(words[11]); // latitude
				Country country = Country.getCountryAt(latitude, longitude);
				if (country != null) {
					for (String tag : tags) {
						tag = tag.trim();
						if (!tag.equals("")) {
							context.write(new Text(country.toString()),
									new StringAndInt(new Text(java.net.URLDecoder.decode(tag)), 1));
						}
					}
				}

			}
		}
		//
		// @Override
		// protected void setup(Context context) {
		// blankLineCounter = context.getCounter(MyCounter.BlankLine);
		// hashMap = new HashMap<String, Integer>();
		// }
		//
		// @Override
		// protected void cleanup(Context context) throws IOException,
		// InterruptedException {
		// for(String key : hashMap.keySet()) {
		// //context.write(new Text(key), new IntWritable(hashMap.get(key)));
		// }
		// }
	}

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {
		private MinMaxPriorityQueue<StringAndInt> minmax;

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> countriesTags = new HashMap<>();
			for (StringAndInt value : values) {
				if (countriesTags.containsKey(value.tag)) {
					countriesTags.put(value.tag.toString(),
							countriesTags.get(value.tag) + value.number);
				} else {
					countriesTags.put(value.tag.toString(), value.number);
				}
			}
			Integer k = context.getConfiguration().getInt("k",0);
			minmax = MinMaxPriorityQueue.maximumSize(k).create();
			for (String t : countriesTags.keySet()) {
				minmax.add(new StringAndInt(new Text(t), countriesTags.get(t)));
			}
			while (!minmax.isEmpty()) {
				StringAndInt si = minmax.poll();
				context.write(key, new Text(si.tag));
			}
		}
	}

	public static class MyCombiner extends
			Reducer<Text, StringAndInt, Text, StringAndInt> {
		
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values,
				Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> countriesTags = new HashMap<>();
			for (StringAndInt value : values) {
				if (countriesTags.containsKey(value.tag)) {
					countriesTags.put(value.tag.toString(),
							countriesTags.get(value.tag) + value.number);
				} else {
					countriesTags.put(value.tag.toString(), value.number);
				}
			}

			for (String t : countriesTags.keySet()) {
				context.write(key,  new StringAndInt(new Text(t), countriesTags.get(t)) );
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		Integer k = new Integer(otherArgs[2]);

		conf.setInt("k", k);

		Job job = Job.getInstance(conf, "Question2_2");
		job.setNumReduceTasks(1);
		job.setJarByClass(Question2_2.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setCombinerClass(MyCombiner.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}