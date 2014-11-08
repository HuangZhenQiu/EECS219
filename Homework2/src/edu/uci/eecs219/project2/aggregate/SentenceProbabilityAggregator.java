package edu.uci.eecs219.project2.aggregate;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.net.URI;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SentenceProbabilityAggregator {
	public static class SentenceProbabilityAggregatorMapper extends Mapper<Text, Text, Text, Text> {
		private Map<Integer, Integer> positionNumberMap = new HashMap<Integer, Integer>();
		
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
				if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
					URI[] files = context.getCacheFiles();
					for (URI file :files) {
						loadPositionNumberMap(file);
					}
				}
				super.setup(context);
		}
		
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<Integer, List<Integer>> positionSidMap = new HashMap<Integer, List<Integer>>();
			StringTokenizer tokenizer = new StringTokenizer(value.toString().trim());
			while (tokenizer.hasMoreTokens()) {
				Integer sentenceId = Integer.parseInt(tokenizer.nextToken());
				if (tokenizer.hasMoreTokens()) {
					Integer position = Integer.parseInt(tokenizer.nextToken());
					List<Integer> sids = positionSidMap.get(position);
					if (sids == null) {
						sids = new ArrayList<Integer>();
						positionSidMap.put(position, sids);
					} else {
						sids.add(sentenceId);
					}
				}
			}
			
			for (Entry<Integer, List<Integer>> entry : positionSidMap.entrySet()) {
				if (positionNumberMap.get(entry.getKey()) != null) {
					Float probability = entry.getValue().size() / new Float(positionNumberMap.get(entry.getKey()));
					for (Integer sid : entry.getValue()) {
						// Let all of the words aggregate on the sentence id, their probability is also passed to reducer.
						context.write(new Text(sid.toString()), new Text(entry.getKey() + " " + key.toString() + " " + probability.toString() + " "));
					}
				}
			}
		
		}
		
		
		// Build the look up hashmap for calculate the probability of word in particular position 
		private void loadPositionNumberMap(URI fileURI) {
			try {
				File file = new File(fileURI);
				
				BufferedReader reader = new BufferedReader(new FileReader(file));
				try {
					String content = reader.readLine();
					while(content != "" && content != null) {
						StringTokenizer tokenizer = new StringTokenizer(content.trim());
						Integer position = Integer.parseInt(tokenizer.nextToken());
						Integer number = Integer.parseInt(tokenizer.nextToken());
						positionNumberMap.put(position, number);
						content = reader.readLine();
					}
				} finally {
					reader.close();
				}
			} catch (Exception e) {
				System.out.println(e.toString());
			}
			
		}
	}
	
	public static class SentenceProbabilityAggregatorReducer extends Reducer<Text, Text, Text, Text> {
				
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer buffer = new StringBuffer();
			for (Text text : values) {
				buffer.append(text.toString());
				buffer.append(" ");
			}
			
			context.write(key, new Text(buffer.toString()));
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
}
