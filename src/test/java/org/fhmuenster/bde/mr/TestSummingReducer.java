package org.fhmuenster.bde.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.fhmuenster.bde.mr.wordcount.SummingReducer;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class TestSummingReducer {

  @Test
  public void returnsSumOfValues()
  throws IOException, InterruptedException {
    new ReduceDriver<Text, IntWritable, Text, IntWritable>()
      .withReducer(new SummingReducer())
      .withInput(new Text("aber"), Arrays.asList(new IntWritable(1), new IntWritable(5)))
      .withOutput(new Text("aber"), new IntWritable(6))
      .runTest();
  }
}
