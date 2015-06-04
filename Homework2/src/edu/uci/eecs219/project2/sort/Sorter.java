package edu.uci.eecs219.project2.sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.math.BigDecimal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Sorter {
	public static class Sentence {
		private String content;
		private BigDecimal probability;
		
		public Sentence(String content, String probability) {
			this.content = content;
			this.probability = new BigDecimal(probability);
		}
		
		public String toString() {
			return probability.toString() + " " + content;
		}
	}
	
	public static class SentenceComparator implements Comparator<Sentence> {

		@Override
		public int compare(Sentence s1, Sentence s2) {
			return s1.probability.compareTo(s2.probability);
		}
	}
	
	public static class SortMapper extends Mapper<Text, Text, Text, Text> {
		private Comparator<Sentence> comparater = new SentenceComparator();
		private PriorityQueue<Sentence> queue = new PriorityQueue<Sentence>(3, comparater);
		
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Sentence sentence = new Sentence(value.toString(), key.toString());
			queue.add(sentence);
			if (queue.size() > 3) {
				queue.poll();
			}
		}
		
		@Override
		public void cleanup(Context context) throws InterruptedException, IOException {
			while(queue.size() > 0) {
				context.write(new Text("1"), new Text(queue.poll().toString()));
			}
		}
	}
	
	public static class SortReducer extends Reducer<Text, Text, Text, Text> {
		private Comparator<Sentence> comparater = new SentenceComparator();
		private PriorityQueue<Sentence> queue = new PriorityQueue<Sentence>(3, comparater);
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String vs = value.toString();
				int index = vs.indexOf(" ");
				Sentence sentence = new Sentence(vs.substring(index + 1), vs.substring(0, index));
				queue.add(sentence);
				if (queue.size() > 3) {
					queue.poll();
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws InterruptedException, IOException {
			while(queue.size() > 0) {
				Sentence sentence1 = queue.poll();
				Sentence sentence2 = queue.poll();
				Sentence sentence3 = queue.poll();
				context.write(new Text(sentence3.content), new Text(sentence3.probability.toPlainString()));
				context.write(new Text(sentence2.content), new Text(sentence2.probability.toPlainString()));
				context.write(new Text(sentence1.content), new Text(sentence1.probability.toPlainString()));
			}
		}
	}
}
