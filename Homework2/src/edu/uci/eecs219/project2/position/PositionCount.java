package edu.uci.eecs219.project2.position;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PositionCount {
	public static class PositionCountMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String sensence = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(sensence);
			Integer position = 0;
			while(tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				context.write(new Text(word), new Text(key.toString() + " " + position.toString() + " "));
				position ++;
			}
		}
	}
	
	public static class PositionCountReducer extends Reducer<Text, Text, Text, Text> {
				
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String content = "";
			for(Text value : values) {
				content = content + " " +  value.toString();
			}
			context.write(key, new Text(content));
		}
	}
}
