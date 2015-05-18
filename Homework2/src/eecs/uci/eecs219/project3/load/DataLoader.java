package eecs.uci.eecs219.project3.load;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException; 

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
				if (lineArr.length >= 10) {
		
					put.add(Bytes.toBytes("Area"), Bytes.toBytes("X1"),
					Bytes.toBytes(lineArr[0]));
					put.add(Bytes.toBytes("Area"), Bytes.toBytes("X5"),
					Bytes.toBytes(lineArr[1]));
					put.add(Bytes.toBytes("Area"), Bytes.toBytes("X6"),
					Bytes.toBytes(lineArr[2]));
					put.add(Bytes.toBytes("Area"), Bytes.toBytes("Y1"),
					Bytes.toBytes(lineArr[3]));
					put.add(Bytes.toBytes("Area"), Bytes.toBytes("Y2"),
					Bytes.toBytes(lineArr[4]));
					put.add(Bytes.toBytes("Property"), Bytes.toBytes("X2"),
					Bytes.toBytes(lineArr[5]));
					put.add(Bytes.toBytes("Property"), Bytes.toBytes("X3"),
					Bytes.toBytes(lineArr[6]));
					put.add(Bytes.toBytes("Property"), Bytes.toBytes("X4"),
					Bytes.toBytes(lineArr[7]));
					put.add(Bytes.toBytes("Property"), Bytes.toBytes("X7"),
					Bytes.toBytes(lineArr[8]));
					put.add(Bytes.toBytes("Property"), Bytes.toBytes("X8"),
							Bytes.toBytes(lineArr[9]));
		
				} else {
					System.out.println("Data is in incorrect format. ");
				}
				context.write(new Text(key.toString()), put);
			}
		}
	}
}
