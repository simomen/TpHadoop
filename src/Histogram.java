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

public class Histogram {

	public static enum MyCounter {
		BlankLine
	}	

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private HashMap<String, Integer> hashMap;
		private Counter blankLineCounter;

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (String line : value.toString().split("\\n+")) {
				int nbTag = 0;
				String[] words = line.split("\\t");
				String[] tags = words[8].split(",");
				Double longitude = Double.valueOf(words[10]); // longitude
				Double latitude = Double.valueOf(words[11]); // latitude
				Country country = Country.getCountryAt(latitude, longitude);
				if (country != null) {
					for (String tag : tags) {
						tag = tag.trim();
						if (!tag.equals("")) {
							nbTag++;
						}
					}
					context.write(new Text(country.toString()), new IntWritable(nbTag));
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private MinMaxPriorityQueue<StringAndInt> minmax;

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			IntWritable val = null;
			for (IntWritable value : values) {
				val = value;
			}

			context.write(key, val);
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

		Job job = Job.getInstance(conf, "Histogram");
		job.setNumReduceTasks(1);
		job.setJarByClass(Histogram.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}