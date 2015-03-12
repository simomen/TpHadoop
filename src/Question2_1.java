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

import java.net.URLDecoder;

public class Question2_1 {

	public static enum MyCounter {
		BlankLine
	}

	public static Integer k;

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

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
									new Text(java.net.URLDecoder.decode(tag)));
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

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private MinMaxPriorityQueue<StringAndInt> minmax;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> countriesTags = new HashMap<>();
			for (Text value : values) {
				if (countriesTags.containsKey(value)) {
					countriesTags.put(value.toString(),
							countriesTags.get(value) + 1);
				} else {
					countriesTags.put(value.toString(), 1);
				}
			}

			minmax = MinMaxPriorityQueue.maximumSize(k).create();
			for (String t : countriesTags.keySet()) {
				minmax.add(new StringAndInt(new Text(t), countriesTags.get(t)));
			}
			while (!minmax.isEmpty()) {
				StringAndInt si = minmax.poll();
				context.write(key, si.tag);
			}
		}

		public class StringAndInt implements Comparable<StringAndInt> {
			Integer number;
			Text tag;

			public StringAndInt(Text tag, Integer i) {
				this.tag = tag;
				this.number = i;
			}

			@Override
			public int compareTo(StringAndInt o) {
				return -this.number.compareTo(o.number);
			}

			@Override
			public String toString() {
				return "StringAndInt [number=" + number + ", tag=" + tag + "]";
			}

		}
	}

	/*
	 * public static class MyCombiner extends Reducer<Text, IntWritable, Text,
	 * IntWritable> {
	 * 
	 * @Override protected void reduce(Text key, Iterable<IntWritable> values,
	 * Context context) throws IOException, InterruptedException { int sum = 0;
	 * for (IntWritable value : values) { sum += value.get(); }
	 * context.write(key, new IntWritable(sum)); } }
	 */

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		k = new Integer(otherArgs[2]);

		Job job = Job.getInstance(conf, "Question1_7");
		job.setNumReduceTasks(1);
		job.setJarByClass(Question2_1.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		// job.setCombinerClass(MyCombiner.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}