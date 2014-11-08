package edu.uci.eecs219.project2.aggregate;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SentenceProbabilityAggregator {
	public static class SentenceProbabilityAggregatorMapper extends Mapper<Text, Text, Text, IntWritable> {
		private Map<Integer, Integer> positionNumberMap;
		
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
				if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
					URI[] files = context.getCacheFiles();
				}
				super.setup(context);
		}
		
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
		
		}
		
		private void loadPositionNumberMap(URI fileURI) {
			File file = new File(fileURI);
		}
	}
	
	public static class SentenceProbabilityAggregatorReducer extends Reducer<Text, IntWritable, Text, Text> {
		private Map<Integer, List<Integer>> posSentenceMap = new HashMap<Integer, List<Integer>>();
				
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		}
	}
}
