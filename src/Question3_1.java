import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Question3_1 {

	public static enum MyCounter {
		BlankLine
	}	

	public static class MyMapper1 extends Mapper<LongWritable, Text, PaysAndTags, IntWritable> {


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
							context.write(new PaysAndTags(country.toString(),java.net.URLDecoder.decode(tag)), new IntWritable(1));
						}
					}
				}

			}
		}
	}

	public static class MyMapper2 extends Mapper<PaysAndTags, IntWritable, StringAndInt, Text> {

		@Override
		protected void map(PaysAndTags key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			Text tag = new Text(key.tag);
			StringAndInt si = new StringAndInt(key.pays, value);			
			context.write(si, tag);						
		}
	}
	
	public static class MyReducer1 extends Reducer<PaysAndTags, IntWritable, PaysAndTags, IntWritable> {

		@Override
		protected void reduce(PaysAndTags key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> countriesTags = new HashMap<>();
			Integer s = 0;
			for (IntWritable value : values) {
				s += value.get();				
			}
			context.write(key, new IntWritable(s));
		}
	}
	
	public static class MyReducer2 extends Reducer<StringAndInt, Text, Text, Text> {
		private MinMaxPriorityQueue<StringAndInt> minmax;
		@Override
		protected void reduce(StringAndInt key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Integer k = context.getConfiguration().getInt("k",0);

			java.util.Iterator<Text> j =  values.iterator();
			for(int i = 0; i < k && j.hasNext(); i++) {
				context.write(new Text(key.tag), j.next());
			}
		}
	}
	
	public static class MyComparator extends WritableComparator {
		public MyComparator() {
			super (StringAndInt.class, true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StringAndInt s1 = (StringAndInt) a;
			StringAndInt s2 = (StringAndInt) b;
			int res =s1.tag.compareTo(s2.tag);
			if (res == 0) return -s1.number.compareTo(s2.number);
			else return res;
		}
	}
	
	public static class MyGroupor extends WritableComparator {
		public MyGroupor() {
			super (StringAndInt.class, true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StringAndInt s1 = (StringAndInt) a;
			StringAndInt s2 = (StringAndInt) b;

			return s1.tag.compareTo(s2.tag);
		}
	}
	

	public static class MyCombiner1 extends
	Reducer<PaysAndTags, IntWritable, PaysAndTags, IntWritable> {
		@Override
		protected void reduce(PaysAndTags key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> countriesTags = new HashMap<>();
			Integer s = 0;
			for (IntWritable value : values) {
				s += value.get();				
			}
			context.write(key, new IntWritable(s));
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

		Job job = Job.getInstance(conf, "Question3_1");
		job.setNumReduceTasks(1);
		job.setJarByClass(Question3_1.class);

		job.setMapperClass(MyMapper1.class);
		job.setMapOutputKeyClass(PaysAndTags.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer1.class);
		job.setOutputKeyClass(PaysAndTags.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output+".tmp"));

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setCombinerClass(MyCombiner1.class);
		
		
		Job job2 = Job.getInstance(conf, "Question3_1");
		job2.setNumReduceTasks(1);
		job2.setJarByClass(Question3_1.class);
		
		job2.setSortComparatorClass(MyComparator.class);
		job2.setGroupingComparatorClass(MyGroupor.class);
			
		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(StringAndInt.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(output+".tmp"));
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(output));

		job2.setOutputFormatClass(TextOutputFormat.class);
		
		if ( job.waitForCompletion(true)) 
			if (job2.waitForCompletion(true))
				System.exit(0);
			else
				System.exit(2);
		else
			System.exit(1);
	}
}