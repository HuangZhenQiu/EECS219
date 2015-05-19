package edu.uci.eecs219.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {

	public static void addRecord(HTable table,
			String rowKey, String family, String qualifier, String value) throws IOException{
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
	}
	
	public static void buildPutForRow(Put put, List<Double> values) {
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X1"),
				Bytes.toBytes(values.get(0).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X5"),
		Bytes.toBytes(values.get(1).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X6"),
		Bytes.toBytes(values.get(2).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("Y1"),
		Bytes.toBytes(values.get(3).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("Y2"),
		Bytes.toBytes(values.get(4).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X2"),
		Bytes.toBytes(values.get(5).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X3"),
		Bytes.toBytes(values.get(6).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X4"),
		Bytes.toBytes(values.get(7).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X7"),
		Bytes.toBytes(values.get(8).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X8"),
				Bytes.toBytes(values.get(9).toString()));
		
	}
	
	
	public static KeyValue[] getRow(HTable table, String rowKey) throws IOException {
		Get get = new Get(rowKey.getBytes());
		Result result = table.get(get);
		return result.raw();
	}
	
	public static Double distance(List<Double> values1, KeyValue[] values2) {
		if (values1.size() != values2.length) {
			return Double.MAX_VALUE;
		}
		
		int length = values2.length;
		Double deviation = 0D;
		for (int i = 0; i < length; i++) {
			Double value2 = new Double(values2[i].getValue().toString());
			deviation = Math.pow(values1.get(i) - value2, 2);
		}
		
		return Math.sqrt(deviation / length);
	}
	
	public static Double distance(KeyValue[] values1, KeyValue[] values2) {
		if (values1.length != values2.length) {
			return Double.MAX_VALUE;
		}
		
		int length = values1.length;
		Double deviation = 0D;
		for (int i = 0; i < length; i++) {
			Double value1 = new Double(values1[i].getValue().toString());
			Double value2 = new Double(values1[i].getValue().toString());
			deviation = Math.pow(value1 - value2, 2);
		}
		
		return Math.sqrt(deviation / length);
	}
	
	public static Map<String, KeyValue[]> loadFromHTable(String tableName) throws IOException {
		Map<String, KeyValue[]> centers = new HashMap<String, KeyValue[]>();
		Configuration config = HBaseConfiguration.create();
		HTable currentCenter = new HTable(config, tableName);
		Scan s = new Scan();
		ResultScanner scanner =  currentCenter.getScanner(s);
		
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			String key = rr.getRow().toString();
			KeyValue[] values = rr.raw();
			centers.put(key, values);
		}
		
		return centers;
	}
	
	
	public static String findNearestPoint(Map<String, KeyValue[]> centers, KeyValue[] point) {
		String neastId = "";
		Double distance = Double.MAX_VALUE;
		for (Entry<String, KeyValue[]> center : centers.entrySet()) {
			Double dis = distance(center.getValue(), point);
			if (dis < distance) {
				distance = dis;
				neastId = center.getKey();
			}
		}
		
		return neastId;
	}
	
	public static List<Double> findCenter(Iterable<Result> results) {
		int number = 0;
		List<Double> centerValues = new ArrayList<Double>();
		for (Result result : results) {
			number ++ ;
			KeyValue[] values = result.raw();
			for (int i =0; i < values.length; i++) {
				if (centerValues.get(i) != null) {
					centerValues.set(i, centerValues.get(i) + new Double(values[i].getValue().toString()));
				} else {
					centerValues.set(i, new Double(values[i].getValue().toString()));
				}
			}
		}
		
		return centerValues;
	}
}
