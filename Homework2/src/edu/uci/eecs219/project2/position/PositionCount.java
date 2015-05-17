package edu.uci.eecs219.project2.position;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PositionCount {
	public static class PositionCountMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Long sentenceId = key.get();
			String sensence = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(sensence);
			Integer position = 0;
			while(tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				context.write(new Text(word), new Text(sentenceId.toString() + " " + position.toString() + " "));
			}
		}
	}
	
	public static class PositionCountReducer extends Reducer<Text, Text, Text, Text> {
		private Map<Integer, List<Integer>> posSentenceMap = new HashMap<Integer, List<Integer>>();
				
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			posSentenceMap.clear();
			String content = "";
			for(Text value : values) {
				content = value.toString();
			}
			context.write(key, new Text(content));
		}
	}
}
