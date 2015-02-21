package ankit.stocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class StockMarket_phase3 {

	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			/* Value will be separated by a tab: 
			   "ANTH-1-2014-12		3.342) */
			int tabSpaceIndex = line.indexOf('\t');
			if (tabSpaceIndex > 0) {
				key1.set(line.substring(0,tabSpaceIndex));
				value1.set(line.substring(tabSpaceIndex+1));
			} else {
				System.err.println("There was a problem in map3");
			}
			
			/* FLIP the key and value */
			context.write(value1, key1);
		}
	}

	public static class Reduce3 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String value = null;
			for (Text text: values)
				value = text.toString();
			
			context.write(new Text(value), key);
		}
	}
}