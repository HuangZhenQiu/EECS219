package edu.uci.eecs219.project2;

import edu.uci.eecs219.project2.position.PositionCount.*;
import edu.uci.eecs219.project2.count.WordPositionSentenceCount.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LocalDriver extends Configured implements Tool{
	private static final String SENTENCE_COUNT_RESULT_PATH = "count";
	private static final String CACHE_FILE = "part-r-00000";
	
	public int run(String[] args) throws Exception
	{
		if(args.length != 2) {
			System.err.println("Usage: MaxTemperatureDriver <input path> <outputpath>");
			System.exit(-1);
		}
		Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		
		Job sCounterJob = new Job();
		sCounterJob.setJarByClass(LocalDriver.class);
		sCounterJob.setInputFormatClass(KeyValueTextInputFormat.class);
		sCounterJob.setJobName("Calculate how many sentences has particular position");
		FileInputFormat.addInputPath(sCounterJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sCounterJob, new Path(SENTENCE_COUNT_RESULT_PATH));
		
		sCounterJob.setMapperClass(WordPositionSentenceCountMapper.class);
		sCounterJob.setReducerClass(WordPositionSentenceCountReducer.class);
		sCounterJob.setOutputKeyClass(Text.class);
		sCounterJob.setOutputValueClass(IntWritable.class);
		sCounterJob.waitForCompletion(true);
		
		Job pCounterJob = new Job();
		pCounterJob.setJarByClass(LocalDriver.class);
		pCounterJob.setInputFormatClass(KeyValueTextInputFormat.class);
		pCounterJob.setJobName("Calculate how many sentences ");
		
		FileInputFormat.addInputPath(pCounterJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(pCounterJob, new Path(args[1]));
		
		pCounterJob.setMapperClass(PositionCountMapper.class);
		pCounterJob.setReducerClass(PositionCountReducer.class);
		
		pCounterJob.setOutputKeyClass(Text.class);
		pCounterJob.setOutputValueClass(Text.class);
		
		
		
		
		
		
		boolean success = pCounterJob.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		LocalDriver driver = new LocalDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
