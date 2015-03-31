package src.country;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Question2_1 {

	public static int k;

	public static class CountryMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@SuppressWarnings("deprecation")
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				for (String line : value.toString().split("\\n")) {
					String mots[] = line.toString().split("\\t");
					double longitude;
					double latitude;

					longitude = Double.parseDouble(mots[10]);
					latitude = Double.parseDouble(mots[11]);

					String tags = mots[8];
					Country country = Country.getCountryAt(latitude, longitude);

					if (country != null) {
						for (String tag : tags.split(",")) {
							tag = tag.trim();
							if (!tag.equals("")) {
								context.write(
										new Text(country.toString()),
										new Text(java.net.URLDecoder
												.decode(tag)));
							}
						}
					}
				}
			} catch (NumberFormatException e) {

			}
		}
	}

	public static class CountryReducer extends Reducer<Text, Text, Text, Text> {
		MinMaxPriorityQueue<StringAndInt> minMaxPriorityQueue = MinMaxPriorityQueue
				.maximumSize(k).create();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> mapTags = new HashMap<>();
			for (Text value : values) {
				if (mapTags.containsKey(value)) {
					mapTags.put(value.toString(), mapTags.get(value) + 1);
				} else {
					mapTags.put(value.toString(), 1);
				}
			}

			StringAndInt stringAndInt;

			for (String t : mapTags.keySet()) {
				stringAndInt = new StringAndInt(new Text(t), mapTags.get(t));
				minMaxPriorityQueue.add(stringAndInt);
			}
			while (!minMaxPriorityQueue.isEmpty()) {
				stringAndInt = minMaxPriorityQueue.poll();
				context.write(key, stringAndInt.tag);
			}
		}
	}

	public static class StringAndInt implements Comparable<StringAndInt> {

		Text tag;
		Integer occurrence;

		public StringAndInt(Text tag, Integer occurence) {
			this.tag = tag;
			this.occurrence = occurence;
		}

		@Override
		public int compareTo(StringAndInt arg0) {

			return -this.occurrence.compareTo(arg0.occurrence);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		k = Integer.valueOf(args[2]);

		Job job = Job.getInstance(conf, "CountryMain");
		job.setJarByClass(Question2_1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CountryMapper.class);
		job.setReducerClass(CountryReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}