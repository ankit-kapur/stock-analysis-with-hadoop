package ankit.stocks;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class StockMarket_phase3 {

	static int counter=1;
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			/* Value will be separated by a tab: 
			   "ANTH-1		3.342) */
			int tabSpaceIndex = line.indexOf('\t');
			if (tabSpaceIndex > 0) {
				key1.set(line.substring(0,tabSpaceIndex));
				value1.set(line.substring(tabSpaceIndex+1));
			} else {
				System.err.println("Error: There was a problem in map3 --> No tab separator");
				System.exit(0);
			}
			
			/* FLIP the key and value */
			context.write(value1, key1);
		}
	}

	public static class Reduce3 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int numOfStocks = 0;
			List<String> stockNameList = new ArrayList<String>();

			for (Text text: values) {
				/* There may be a rare chance that multiple 
				 * stocks have the same volatility value */
				stockNameList.add(text.toString());
			}
			
			Configuration conf = context.getConfiguration();
			String numOfStocksStr = conf.get("numOfStocks");
			if (MainStockMarket.isNumeric(numOfStocksStr))
				numOfStocks = Integer.parseInt(numOfStocksStr);
			
			for (String stockName: stockNameList) {
				if (counter == 1) {
					context.write(new Text("   "), new Text("      "));
					context.write(new Text("--== Lowest volatility stocks ==--"), new Text(""));
				}
				if (counter == 11) {
					context.write(new Text("   "), new Text("      "));
					context.write(new Text("--== Highest volatility stocks ==--"), new Text(""));
				}
				
				/* If it's in the top 10 or bottom 10, write it */
				if (counter <= 10 || counter > numOfStocks-10) {
					if (MainStockMarket.isDouble(key.toString())) {
						double volatility = Double.parseDouble(key.toString());
						DecimalFormat myFormatter = new DecimalFormat("0.0000000");
						String volatilityFormatted = myFormatter.format(volatility);
						context.write(new Text(stockName), new Text(volatilityFormatted));
					}
				}
				counter++;
			}
		}
	}
}