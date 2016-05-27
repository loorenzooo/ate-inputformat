package fr.ibp.hadoop.mapred;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionInputStream;

public class AteLineReader {

	public AteLineReader(CompressionInputStream createInputStream, Configuration job, byte[] recordDelimiter) {
		// TODO Auto-generated constructor stub
	}

	public AteLineReader(FSDataInputStream fileIn, Configuration job, byte[] recordDelimiter) {
		// TODO Auto-generated constructor stub
	}

	public AteLineReader(InputStream in, byte[] recordDelimiter) {
		// TODO Auto-generated constructor stub
	}

	public AteLineReader(InputStream in, Configuration job, byte[] recordDelimiter) {
		// TODO Auto-generated constructor stub
	}

	public int readLine(Text text, int i, int maxBytesToConsume) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean needAdditionalRecordAfterSplit() {
		// TODO Auto-generated method stub
		return false;
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

}
