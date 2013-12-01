package org.fhmuenster.bde.mr;

import org.apache.hadoop.util.ProgramDriver;
import org.fhmuenster.bde.mr.tfidf.TFIDFJobController;
import org.fhmuenster.bde.mr.wordcount.WordCount;

public class Driver {

  public static void main(String argv[]) {
 		int exitCode = -1;
 		ProgramDriver pgd = new ProgramDriver();
 		try {
 			pgd.addClass("wordcount", WordCount.class,
 					"MapReduce program to count word frequencies.");
      pgd.addClass("tfidf", TFIDFJobController.class,
   					"MapReduce program to compute TF-IDF of input text files.");
 			pgd.driver(argv);
 			// Success
 			exitCode = 0;
 		} catch (Throwable e) {
 			e.printStackTrace();
 		}
 		System.exit(exitCode);
 	}
}
