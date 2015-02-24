package ankit.stocks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StockMarket_phase1 {

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = null;
			/* --- Get the file name --- */
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();

			/* Remove the .csv from the file name */
			if (fileName.indexOf(".csv") >= 0)
				fileName = fileName.substring(0, fileName.indexOf(".csv"));

			if (value != null && (line = value.toString()).length() > 0) {
				
				/* A line of data looks like this: "2014-12-31,1.58,1.66,1.50,1.58,236500,1.58" */

				/* --- Get the date --- */
				/* If this line is not a header, it won't contain a '-' */
				if (line.indexOf('-') >= 0) {
					String date = line.substring(0, line.indexOf(','));
					String yearMonth = date.substring(0, date.lastIndexOf('-'));

					/* They key will look like this: "ANTH-2014-12" */
					key1.set(fileName + "-" + yearMonth);
					/* The value will be the line as is */
					value1.set(line);

					/* How a (key, value) pair will look: ("ANTH-2014-12",
					 * "2014-12-31,1.58,1.66,1.50,1.58,236500,1.58") */
					context.write(key1, value1);
				}
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		private Text key2 = new Text();
		private Text value2 = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			/* How an input (key, value) pair will look: ("ANTH-2014-12",
			 * "2014-12-31,1.58,1.66,1.50,1.58,236500,1.58") */

			int firstDay = 32;
			int lastDay = 0;
			String firstDayData = "", lastDayData = "";

			for (Text value : values) {
				/* Get the DAY of the month */
				String line = value.toString();
				String date = line.substring(0, line.indexOf(','));
				String dayOfTheMonth = date.substring(
						date.lastIndexOf('-') + 1, date.length());
				int day = Integer.parseInt(dayOfTheMonth);

				if (day < firstDay) {
					firstDay = day;
					firstDayData = line;
				}
				if (day > lastDay) {
					lastDay = day;
					lastDayData = line;
				}
			}

			/* Now, we've found the data for the 1st and last day of the month.
			 * Calculate xi */
			double adjBegin = Double.parseDouble(firstDayData
					.substring(firstDayData.lastIndexOf(',') + 1));
			double adjEnd = Double.parseDouble(lastDayData
					.substring(lastDayData.lastIndexOf(',') + 1));

			/* Apply the formula for xi */
			double xi = (adjEnd - adjBegin) / adjBegin;

			/* Set the string as the same thing we received
			 * "STOCKNAME-MONTH-YEAR" */
			key2.set(key.toString());
			/* Set the value as xi */
			value2.set(Double.toString(xi));
			context.write(key2, value2);
		}
	}
}