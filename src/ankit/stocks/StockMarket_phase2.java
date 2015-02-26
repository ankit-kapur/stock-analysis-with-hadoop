package ankit.stocks;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class StockMarket_phase2 {

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String keyString = "", valueString = "";

			/* Value will be separated by a tab: "ANTH-1-2014-12 3.342)	 */
			int tabSpaceIndex = line.indexOf('\t');
			if (tabSpaceIndex > 0) {
				keyString = line.substring(0, tabSpaceIndex);
				valueString = line.substring(tabSpaceIndex + 1);

				/* --- Extract just the stockname, i.e. "ANTH-1" --- */
				/* First remove the month */
				keyString = keyString.substring(0, keyString.lastIndexOf('-'));
				/* Then the year */
				keyString = keyString.substring(0, keyString.lastIndexOf('-'));
			} else {
				System.err.println("There was a problem in map2");
			}

			/* The key is now just the stock-name */
			key1.set(keyString);
			/* We send the xi value as-is */
			value1.set(valueString);
			context.write(key1, value1);
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			/* What we get: (STOCK-NAME, xi values) */
			int N = 0;
			double xbar = 0, volatility = 0, xiSum = 0;
			List<Double> xiList = new ArrayList<Double>();
			for (Text value : values) {
				String xiString = value.toString();
				double xi = Double.parseDouble(xiString);

				xiList.add(xi);
				xiSum += xi;
				N++;
			}

			/* If N is 0 it means there's no stock data for this company */
			if (N > 0) {
				/* Calculate xbar */
				xbar = xiSum / N;

				double otherSum = 0.0;
				for (double xi : xiList) {
					otherSum += Math.pow(xi - xbar, 2);
				}

				/* Apply the volatility formula */
				if ((N-1) != 0)
					volatility = Math.sqrt((otherSum) / (N - 1));
				
				if (volatility > 0) {
					/* ----- Increment the stock counter ----- */
					context.getCounter(MainStockMarket.STOCK_COUNTER.NUM_OF_STOCKS).increment(1);
					
					DecimalFormat myFormatter = new DecimalFormat("0.000000000");
					String volatilityFormatted = myFormatter.format(volatility);
					
					/* Write the volatility value */
					context.write(key, new Text(volatilityFormatted));
				}
			}
		}
	}
}