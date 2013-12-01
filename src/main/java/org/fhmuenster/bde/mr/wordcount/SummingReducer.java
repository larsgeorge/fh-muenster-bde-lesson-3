package org.fhmuenster.bde.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SummingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable sum = new IntWritable();

  @Override
  protected void reduce(Text word, Iterable<IntWritable> values,
    Context context) throws IOException, InterruptedException {
    int total = 0;
    for (IntWritable val : values) {
      total += val.get();
    }
    sum.set(total);
    context.write(word, sum);
  }
}
