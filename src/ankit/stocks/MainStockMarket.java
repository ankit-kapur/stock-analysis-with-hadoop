package ankit.stocks;

import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainStockMarket {

	public static void main(String[] args) throws Exception {
		
		long startTime = new Date().getTime();		

		System.out.println("\n===------- Start --> Stock volatility estimation -------===\n");

		/* --- Job 1 configuration --- */
	    Job job1 = Job.getInstance();
	    job1.setJarByClass(StockMarket_phase1.class);
		job1.setMapperClass(StockMarket_phase1.Map1.class);
		job1.setReducerClass(StockMarket_phase1.Reduce1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		/* --- Job 2 configuration --- */
	    Job job2 = Job.getInstance();
	    job2.setJarByClass(StockMarket_phase2.class);
		job2.setMapperClass(StockMarket_phase2.Map2.class);
		job2.setReducerClass(StockMarket_phase2.Reduce2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		/* --- Job 3 configuration --- */
	    Job job3 = Job.getInstance();
	    job3.setJarByClass(StockMarket_phase3.class);
		job3.setMapperClass(StockMarket_phase3.Map3.class);
		job3.setReducerClass(StockMarket_phase3.Reduce3.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
//		int NOfReducer1 = Integer.valueOf(args[1]);	
//		job1.setNumReduceTasks(NOfReducer1);
//		int NOfReducer2 = Integer.valueOf(args[1]);
//		job2.setNumReduceTasks(NOfReducer2);
		
		/* ---- Configuring I/O paths ---- */
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("Intermediate1"));
		
		FileInputFormat.addInputPath(job2, new Path("Intermediate1"));
		FileOutputFormat.setOutputPath(job2, new Path("Intermediate2"));

		FileInputFormat.addInputPath(job3, new Path("Intermediate2"));
		FileOutputFormat.setOutputPath(job3, new Path("Output"));
		
		/* Wait for completion of each job */
		boolean status;
		status = job1.waitForCompletion(true);
		status = job2.waitForCompletion(true);
		status = job3.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nJob took " + (end-startTime)/1000.0 + "seconds\n");
		}
		System.out.println("\n -------=== Completed --> Stock volatility estimation ===------- \n");		
	}
}
