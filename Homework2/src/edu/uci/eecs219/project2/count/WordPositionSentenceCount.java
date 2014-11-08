package edu.uci.eecs219.project2.count;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordPositionSentenceCount {
	public static class WordPositionSentenceCountMapper extends Mapper<Text, Text, Text, IntWritable> {
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String sensence = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(sensence);
			Integer position = 0;
			while(tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				context.write(new Text(position.toString()), new IntWritable(1));
				position ++;
			}
		}
	}
	
	public static class WordPositionSentenceCountReducer extends Reducer<Text, IntWritable, Text, Text> {
		private Map<Integer, List<Integer>> posSentenceMap = new HashMap<Integer, List<Integer>>();
				
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Integer sentenceNumber = 0;
			for (IntWritable value : values) {
				sentenceNumber += value.get();
			}
			
			context.write(key, new Text(sentenceNumber.toString()));
		}
	}
}
