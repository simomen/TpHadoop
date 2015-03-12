package src.country;

import java.io.IOException;
import java.net.URLDecoder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountryMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String mots[] = value.toString().split("\\s+");
		double longitude;
		double latitude;

		try {
			longitude = Double.parseDouble(mots[10]);
			latitude = Double.parseDouble(mots[11]);

			String tags = mots[8];
			Country country = Country.getCountryAt(latitude, longitude);

			if (country != null) {
				for (String tag : tags.split(",")) {
					tag = URLDecoder.decode(tag);
					System.out.println(country + " : tag = " + tag);
					context.write(new Text(country.toString()), new Text(tag));
				}
			}

		} catch (NumberFormatException e) {

		}
	}
}
