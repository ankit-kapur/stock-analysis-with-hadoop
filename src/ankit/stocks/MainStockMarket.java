package ankit.stocks;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainStockMarket {

	public static void main(String[] args) throws Exception {
		
		long startTime = new Date().getTime();
		boolean status;
		
		System.out.println("\n===------- Start --> Stock volatility estimation -------===\n");

		/* --- Job 1 configuration --- */
	    Job job1 = Job.getInstance();
	    job1.setJarByClass(StockMarket_phase1.class);
		job1.setMapperClass(StockMarket_phase1.Map1.class);
		job1.setReducerClass(StockMarket_phase1.Reduce1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		/* ---- Configuring I/O paths ---- */
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("Intermediate1"));
		/* No. of reducers */
		job1.setNumReduceTasks(Integer.parseInt(args[2]));
		status = job1.waitForCompletion(true);

		/* --- Job 2 configuration --- */
	    Job job2 = Job.getInstance();
	    job2.setJarByClass(StockMarket_phase2.class);
		job2.setMapperClass(StockMarket_phase2.Map2.class);
		job2.setReducerClass(StockMarket_phase2.Reduce2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		/* ---- Configuring I/O paths ---- */
		FileInputFormat.addInputPath(job2, new Path("Intermediate1"));
		FileOutputFormat.setOutputPath(job2, new Path("Intermediate2"));
		/* No. of reducers */
		job2.setNumReduceTasks(Integer.parseInt(args[3]));
		status = job2.waitForCompletion(true);
		/* Get the value of the stock counter */
		Counters counters = job2.getCounters();
		Counter c1 = counters.findCounter(MainStockMarket.STOCK_COUNTER.NUM_OF_STOCKS);
		long numOfStocks = c1.getValue();
		
		/* --- Job 3 configuration --- */
	    Job job3 = Job.getInstance();
	    job3.setJarByClass(StockMarket_phase3.class);
		job3.setMapperClass(StockMarket_phase3.Map3.class);
		job3.setReducerClass(StockMarket_phase3.Reduce3.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		/* ---- Configuring I/O paths ---- */
		FileInputFormat.addInputPath(job3, new Path("Intermediate2"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		/* No. of reducers */
		job3.setNumReduceTasks(1);
		/* Set number of stocks */
		Configuration conf = job3.getConfiguration();
		conf.set("numOfStocks", Long.toString(numOfStocks));
		status = job3.waitForCompletion(true);
				
		
		/* Record the time taken */
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nTime taken for all jobs to complete: " + (end-startTime)/1000.0 + " seconds\n");
		}
		System.out.println("\n -------=== Completed --> Stock volatility estimation ===------- \n");		
	}
	
	/* A utility function */
	public static boolean isNumeric(String string) {
		if (string == null)
			return false;
		try {
			Integer.parseInt(string);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
	
	/* The counter for number of stocks */
	public static enum STOCK_COUNTER {
		  NUM_OF_STOCKS,
	};
}
