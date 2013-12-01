package org.fhmuenster.bde.mr.tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordsInCorpusTFIDF extends Configured implements Tool {

  private static final String INPUT_PATH = "2-word-counts";

  public static class WordsInCorpusTFIDFMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    private Text wordAndDoc = new Text();
    private Text wordAndCounters = new Text();

    public void map(LongWritable key, Text value,
      Context context) throws IOException, InterruptedException {
      String[] wordAndCounters = value.toString().split("\t");
      String[] wordAndDoc = wordAndCounters[0].split("@");  //3/1500
      this.wordAndDoc.set(new Text(wordAndDoc[0]));
      this.wordAndCounters.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
      context.write(this.wordAndDoc, this.wordAndCounters);
    }
  }

  public static class WordsInCorpusTFIDFReducer extends Reducer<Text, Text, Text, Text> {

    private static final DecimalFormat DF = new DecimalFormat("###.########");
    private Text wordAtDocument = new Text();
    private Text tfidfCounts = new Text();

    protected void reduce(Text key, Iterable<Text> values,
      Context context) throws IOException, InterruptedException {
      int numberOfDocumentsInCorpus =
        context.getConfiguration().getInt("numberOfDocsInCorpus", 0);
      int numberOfDocumentsInCorpusWhereKeyAppears = 0;
      Map<String, String> tempFrequencies = new HashMap<String, String>();
      for (Text val : values) {
        String[] documentAndFrequencies = val.toString().split("=");
        if (Integer.parseInt(documentAndFrequencies[1].split("/")[0]) > 0) {
          numberOfDocumentsInCorpusWhereKeyAppears++;
        }
        tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
      }
      for (String document : tempFrequencies.keySet()) {
        String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
        double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0]) /
          Double.valueOf(wordFrequenceAndTotalWords[1]));
        double idf = Math.log10((double) numberOfDocumentsInCorpus /
          (double) ((numberOfDocumentsInCorpusWhereKeyAppears == 0 ? 1 : 0) +
            numberOfDocumentsInCorpusWhereKeyAppears));
        double tfIdf = tf * idf;
        this.wordAtDocument.set(key + "@" + document);
        this.tfidfCounts.set(
          "[" + numberOfDocumentsInCorpusWhereKeyAppears + "/" +
            numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] +
            "/" + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]");
        context.write(this.wordAtDocument, this.tfidfCounts);
      }
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: tfidf <out> <numfiles>");
      return 2;
    }
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path userOutputPath = new Path(args[0]);
    if (fs.exists(userOutputPath)) {
      fs.delete(userOutputPath, true);
    }
    conf.setInt("numberOfDocsInCorpus", Integer.parseInt(args[1]));

    Job job = new Job(conf, "TF-IDF of Words in Corpus");
    job.setJarByClass(WordsInCorpusTFIDF.class);
    job.setMapperClass(WordsInCorpusTFIDFMapper.class);
    job.setReducerClass(WordsInCorpusTFIDFReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
    FileOutputFormat.setOutputPath(job, userOutputPath);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(),
      new WordsInCorpusTFIDF(), args);
    System.exit(res);
  }
}