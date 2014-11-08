package edu.uci.eecs219.preprocess;

import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class Preprocessor {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if(args.length != 2) {
			System.err.println("Usage: MaxTemperatureDriver <input path> <outputpath>");
			System.exit(-1);
		}

		
		
		try {
			File inFile = new File(args[0]);
			File outFile = new File(args[1]);
			BufferedReader reader = new BufferedReader(new FileReader(inFile));
			BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
			try {
				String content = reader.readLine();
				Integer sid = 0;
				while(content != "" && content!= null) {
					content = sid.toString() + "\t" + content;
					writer.write(content);
					writer.newLine();
					content = reader.readLine();
					sid ++;
				}
			} finally {
				reader.close();
				writer.close();
			}
			
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}

}
