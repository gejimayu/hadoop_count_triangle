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
      	a.set(longA);
        b.set(longB);
      	if (longA < longB) {
        	context.write(a, b);
      	} else {
      		context.write(b, a);
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

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
      throws IOException, InterruptedException
    {
    	Iterator<LongWritable> itrVal = values.iterator();
    	long val1 = itrVal.next().get();
    	rKey.set(key.toString() + "," + Long.toString(val1));
    	context.write(rKey, zero);
    	while (itrVal.hasNext()) {
    		long val2 = itrVal.next().get();
    		rKey.set(key.toString() + "," + Long.toString(val2));
    		context.write(rKey, zero);

  			if (val1 != val2) {
  				if (val1 < val2) {
  					rKey.set(Long.toString(val1) + "," + Long.toString(val2));
  				} else {
  					rKey.set(Long.toString(val2) + "," + Long.toString(val1));
  				}
  				context.write(rKey, one);
  			}
  			val1 = val2;
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
      boolean hasZero = false;
      boolean hasOne = false;
      Iterator<LongWritable> vs = values.iterator();
      // Triad edge value=1, original edge value=0.
      while (vs.hasNext()) {
        long temp = vs.next().get();
        if (temp == 0) {
        	hasZero = true;
        } else {
        	hasOne = true;
        }
      }
      if (hasZero && hasOne) {
      	count++;
      }
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

  //   Job job1 = Job.getInstance(conf, "TAHU BULAT DIGORENG DADAKAN first step");
  //   job1.setJarByClass(CountTriangle.class);
  //   job1.setMapperClass(UndirectedMapper.class);
  //   job1.setReducerClass(TriadsReducer.class);

  //   job1.setMapOutputKeyClass(LongWritable.class);
		// job1.setMapOutputValueClass(LongWritable.class);
  //   job1.setOutputKeyClass(Text.class);
  //   job1.setOutputValueClass(LongWritable.class);

  //   FileInputFormat.addInputPath(job1, new Path(args[0]));
  //   FileOutputFormat.setOutputPath(job1, new Path("/user/m/output1"));

    Job job3 = Job.getInstance(conf, "TAHU BULAT DIGORENG DADAKAN second step");
    job3.setJarByClass(CountTriangle.class);
    job3.setMapperClass(ParseTextLongPairsMapper.class);
    job3.setReducerClass(CountTrianglesReducer.class);

    job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(LongWritable.class);
    job3.setOutputKeyClass(LongWritable.class);
    job3.setOutputValueClass(LongWritable.class);

    FileInputFormat.addInputPath(job3, new Path("/user/m/output1"));
    FileOutputFormat.setOutputPath(job3, new Path("/user/m/output2"));

    Job job4 = Job.getInstance(conf, "TAHU BULAT DIGORENG DADAKAN aggregate results");
    job4.setJarByClass(CountTriangle.class);
    job4.setMapperClass(ParseTextLongPairsMapper.class);
    job4.setReducerClass(AggregateCountsReducer.class);

    job4.setMapOutputKeyClass(Text.class);
    job4.setMapOutputValueClass(LongWritable.class);
    job4.setOutputKeyClass(LongWritable.class);
    job4.setOutputValueClass(LongWritable.class);

    FileInputFormat.setInputPaths(job4, new Path("/user/m/output2"));
		FileOutputFormat.setOutputPath(job4, new Path("/user/m/output3"));

    // int ret = job1.waitForCompletion(true) ? 0 : 1;
    // if (ret==0) ret = job3.waitForCompletion(true) ? 0 : 1;
    int ret = job3.waitForCompletion(true) ? 0 : 1;
    if (ret==0) ret = job4.waitForCompletion(true) ? 0 : 1;
  }
}