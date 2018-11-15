import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class CountTriangle {
  public static class UndirectedMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
    private LongWritable a = new LongWritable();
		private LongWritable b = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.countTokens() == 2) {
        String A = itr.nextToken();
        String B = itr.nextToken();
        long longA = Long.parseLong(A);
      	long longB = Long.parseLong(B);
      	a.set(A);
        b.set(B);
      	if (longA < longB) {
        	context.write(a, b);
      	} else {
      		context.write(b, a);
      	}
      }
    }
  }

  public static class UndirectedReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      boolean []hashSet = new boolean[65000000];
      Text from = new Text(key);
      Text to = new Text();
      int idx;
      for (Text val : values) {
      	String valStr = val.toString();
      	idx = Integer.parseInt(valStr);
      	if (!hashSet[idx]) {
      		hashSet[idx] = true;
	        to.set(valStr);
					context.write(from, to);
      	}
      }
    }
  }

  // Maps values to Long,Long pairs. 
  public static class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
  {
    LongWritable mKey = new LongWritable();
    LongWritable mValue = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
      StringTokenizer tokenizer = new StringTokenizer(value.toString());
      while (tokenizer.countTokens() == 2) {
        long e1 = Long.parseLong(tokenizer.nextToken());
        long e2 = Long.parseLong(tokenizer.nextToken());
        // only need one unique pair
        if (e1 < e2) {
          mKey.set(e1);
          mValue.set(e2);
          context.write(mKey,mValue);
        }
      }
    }
  }

  // Produces original edges and triads.
  public static class TriadsReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable>
  {
    Text rKey = new Text();
    final static LongWritable zero = new LongWritable((byte)0);
    final static LongWritable one = new LongWritable((byte)1);
    long []vArray = new long[65000000];
    int size = 0;

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
      throws IOException, InterruptedException
    {
      // Produce triads - all permutations of pairs where e1 < e2 (value=1).
      // And all original edges (value=0).
      // Sorted by value.
      Iterator<LongWritable> vs = values.iterator();
      for (size = 0; vs.hasNext(); ) {
        if (vArray.length==size) {
          vArray = Arrays.copyOf(vArray, vArray.length*2);
        }

        long e = vs.next().get();
        vArray[size++] = e;

        // Original edge.
        rKey.set(key.toString() + "," + Long.toString(e));
        context.write(rKey, zero);
      }

      Arrays.sort(vArray, 0, size);

      // Generate triads.
      for (int i=0; i<size; ++i) {
        for (int j=i+1; j<size; ++j) {
          rKey.set(Long.toString(vArray[i]) + "," + Long.toString(vArray[j]));
          context.write(rKey, one);
        }
      }
    }
  }

  // Parses values into {Text,Long} pairs.
  public static class ParseTextLongPairsMapper extends Mapper<LongWritable, Text, Text, LongWritable>
  {
    Text mKey = new Text();
    LongWritable mValue = new LongWritable();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      if (tokenizer.hasMoreTokens()) {
        mKey.set(tokenizer.nextToken());
        if (!tokenizer.hasMoreTokens())
            throw new RuntimeException("invalid intermediate line " + line);
        mValue.set(Long.parseLong(tokenizer.nextToken()));
        context.write(mKey, mValue);
      }
    }
  }

  // Counts the number of triangles.
  public static class CountTrianglesReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
  {
    long count = 0;
    final static LongWritable zero = new LongWritable(0);

    public void cleanup(Context context)
        throws IOException, InterruptedException
    {
      LongWritable v = new LongWritable(count);
      if (count > 0) context.write(zero, v);
    }

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException
    {
      boolean isClosed = false;
      long c = 0, n = 0;
      Iterator<LongWritable> vs = values.iterator();
      // Triad edge value=1, original edge value=0.
      while (vs.hasNext()) {
        c += vs.next().get();
        ++n;
      }
      if (c!=n) count += c;
    }
  }

  // Aggregates the counts.
  public static class AggregateCountsReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
  {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException
    {
      long sum = 0;
      Iterator<LongWritable> vs = values.iterator();
      while (vs.hasNext()) {
        sum += vs.next().get();
      }
      context.write(new LongWritable(sum), null);
    }
	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job1 = Job.getInstance(conf, "preprocessing");
    job1.setJarByClass(CountTriangle.class);
    job1.setMapperClass(UndirectedMapper.class);
    job1.setCombinerClass(UndirectedReducer.class);
    job1.setReducerClass(UndirectedReducer.class);

    job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("/user/m/output1"));

    Job job2 = Job.getInstance(conf, "first step");
    job2.setJarByClass(CountTriangle.class);
    job2.setMapperClass(ParseLongLongPairsMapper.class);
    job2.setReducerClass(TriadsReducer.class);

    job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(LongWritable.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(LongWritable.class);

    FileInputFormat.addInputPath(job2, new Path("/user/m/output1"));
    FileOutputFormat.setOutputPath(job2, new Path("/user/m/output2"));

    Job job3 = Job.getInstance(conf, "second step");
    job3.setJarByClass(CountTriangle.class);
    job3.setMapperClass(ParseTextLongPairsMapper.class);
    job3.setReducerClass(CountTrianglesReducer.class);

    job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(LongWritable.class);
    job3.setOutputKeyClass(LongWritable.class);
    job3.setOutputValueClass(LongWritable.class);

    FileInputFormat.addInputPath(job3, new Path("/user/m/output2"));
    FileOutputFormat.setOutputPath(job3, new Path("/user/m/output3"));

    Job job4 = Job.getInstance(conf, "aggregate results");
    job4.setJarByClass(CountTriangle.class);
    job4.setMapperClass(ParseTextLongPairsMapper.class);
    job4.setReducerClass(AggregateCountsReducer.class);

    job4.setMapOutputKeyClass(Text.class);
    job4.setMapOutputValueClass(LongWritable.class);
    job4.setOutputKeyClass(LongWritable.class);
    job4.setOutputValueClass(LongWritable.class);

    FileInputFormat.setInputPaths(job4, new Path("/user/m/output3"));
		FileOutputFormat.setOutputPath(job4, new Path("/user/m/output4"));

    int ret = job1.waitForCompletion(true) ? 0 : 1;
    if (ret==0) ret = job2.waitForCompletion(true) ? 0 : 1;
    if (ret==0) ret = job3.waitForCompletion(true) ? 0 : 1;
    if (ret==0) ret = job4.waitForCompletion(true) ? 0 : 1;
  }
}