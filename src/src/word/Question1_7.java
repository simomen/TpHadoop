package src.word;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class Question1_7 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Question1_7.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class WordCountMapper extends
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

				if (word.equals(""))
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

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}