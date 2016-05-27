package fr.ibp.hadoop.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AteLineReaderTest {
	
	private static final Logger logger = LoggerFactory.getLogger(AteLineReader.class);
	private static final byte CR = '\r';
	private static final byte LF = '\n';
	
	AteLineReader ateLineReader;
	
	@Test
	public void readLineSimpleTest() throws IOException{
		InputStream in = this.getClass().getClassLoader().getResourceAsStream("ExempleSimple.txt");
		ateLineReader = new AteLineReader(in);
		Text text = new Text();
		int i = 0;
		int ligne1 = ateLineReader.readLine(text);
		logger.info("Ligne 1 - longeur : {} - texte : {}{}", ligne1, text,"FIN");
		int ligne2 = ateLineReader.readLine(text);
		logger.info("Ligne 2 - longeur : {} - texte : {}{}", ligne2, text,"FIN");
		Assert.assertTrue(ligne1 > 0);
	}
	
	@Test
	public void readLineCustomDelimiterTest() throws IOException{
		InputStream in = this.getClass().getClassLoader().getResourceAsStream("ExempleCustomDelimiter.txt");
		//byte[] delimiter = new byte[] {1, 1, 2, 3, 5, 8};
		byte[] delimiter = "\r\nPERF".getBytes();
		ateLineReader = new AteLineReader(in,delimiter);
		Text text = new Text();
		int i = 0;
		int ligne1 = ateLineReader.readLine(text);
		logger.info("Ligne 1 - longeur : {} - texte : {}", ligne1, text);
		int ligne2 = ateLineReader.readLine(text);
		logger.info("Ligne 2 - longeur : {} - texte : {}", ligne2, text);
		Assert.assertTrue(ligne1 > 0);
	}
	

}
