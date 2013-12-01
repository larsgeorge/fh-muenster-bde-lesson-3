package org.fhmuenster.bde.mr;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.fhmuenster.bde.mr.wordcount.TokenizingMapper;
import org.junit.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class TestTokenizingReducer {

  @Test
  public void testTokenzingMapper() throws IOException {
    Text value = new Text("Aber, Ende.");
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new TokenizingMapper())
      .withInput(new LongWritable(), value)
      .withOutput(new Text("aber"), new IntWritable(1))
      .withOutput(new Text("ende"), new IntWritable(1))
      .runTest();
  }
}
