package edu.uci.eecs219.project2.aggregate;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.math.BigDecimal;

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
		private static class Word implements Comparable {
			private String content;
			private Integer position;
			private Float probability;
			
			public Word(String content, Integer position, Float probability) {
				this.content = content;
				this.position = position;
				this.probability = probability;  // Normalizer
			}

			@Override
			public int compareTo(Object o) {
				// TODO Auto-generated method stub
				if (o instanceof Word) {
					Word word = (Word) o;
					return this.position.compareTo(word.position);
				}
				return 1;
			}
		}
				
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			BigDecimal sentenceProability =  new BigDecimal(1);
			List<Word> words =  new ArrayList<Word>();
			StringBuffer buffer = new StringBuffer();
			for (Text text : values) {
				StringTokenizer tokenizer = new StringTokenizer(text.toString().trim());
				String position = tokenizer.nextToken();
				String content = tokenizer.nextToken();
				String probability = tokenizer.nextToken();
				words.add(new Word(content, Integer.parseInt(position), Float.parseFloat(probability)));
			}
			
			Collections.sort(words);
			for (Word word : words) {
				buffer.append(word.content);
				buffer.append(" ");
				sentenceProability = sentenceProability.multiply(new BigDecimal(word.probability));
			}
			
			context.write(new Text(sentenceProability.toEngineeringString()), new Text(buffer.toString()));
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
}
