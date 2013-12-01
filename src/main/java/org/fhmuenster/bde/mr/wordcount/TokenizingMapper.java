package org.fhmuenster.bde.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  public enum Counters { UNCHANGED, MODIFIED }

  private final IntWritable ONE = new IntWritable(1);
  private Text word = new Text();

  @Override
  protected void map(LongWritable offset, Text line,
    Context context) throws IOException, InterruptedException {
    StringTokenizer parser = new StringTokenizer(line.toString());
    while (parser.hasMoreTokens()) {
      String oldWord = parser.nextToken();
      String newWord = oldWord.replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();
      context.getCounter(oldWord.equals(newWord) ?
        Counters.UNCHANGED : Counters.MODIFIED).increment(1);
      word.set(newWord);
      context.write(word, ONE);
    }
  }
}
