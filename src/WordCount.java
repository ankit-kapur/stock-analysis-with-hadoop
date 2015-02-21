import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	/**
	 * @author hduser seperate the files into lines and lines into token input:
	 *         <key, value>, key: line number, value: line output: <key, value>,
	 *         key: each word, value: number of occurence e.g. input <line1,
	 *         hello a bye a> output<hello, 1>,<a, 1>, <bye, 1>,<a, 1>
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1); // value = 1
		private Text word = new Text(); // output key

		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line); // based on
																	// space
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				try {
					context.write(word, one);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}

	/**
	 * @author hduser Count the number of times the given keys occur input:
	 *         <key, value>, key = Text, value: number of occurrence output:
	 *         <key, value>, key = Text, value = number of occurrence
	 * 
	 * */
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) {

		System.out.println("HI HOW ARE YOU \n HELLOW HELLOW");
		try {
			// Create a new Job
			Job job = Job.getInstance();
			job.setJarByClass(WordCount.class);

			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setJarByClass(WordCount.class);
			job.waitForCompletion(true);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
