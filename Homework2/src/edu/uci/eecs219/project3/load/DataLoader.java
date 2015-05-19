package edu.uci.eecs219.project3.load;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.uci.eecs219.util.HbaseUtil;

import java.util.List;
import java.io.IOException; 
import java.util.ArrayList;
import java.util.Arrays;

/**
 * 
 * Load data in input text into table.
 * @author hadoop
 *
 */
public class DataLoader {
	public static class DataLoaderMapper extends Mapper<LongWritable, Text, Text, Put> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Put put = new Put(Bytes.toBytes(key.get()));
			if (line != "") {
				String[] lineArr = line.split(" ");
				List<Double> values = new ArrayList<Double>();
				for (String s : lineArr) {
					values.add(Double.parseDouble(s));
				}
				if (lineArr.length >= 10) {
					HbaseUtil.buildPutForRow(put, values);
		
				} else {
					System.out.println("Data is in incorrect format. ");
				}
				context.write(new Text(key.toString()), put);
			}
		}
	}
}
