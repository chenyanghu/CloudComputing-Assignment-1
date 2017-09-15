
	import java.io.BufferedReader;
	import java.io.File;
	import java.io.FileReader;
	import java.io.IOException;
	import java.net.URI;
	import java.util.HashSet;
	import java.util.Set;
	import java.util.StringTokenizer;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.util.GenericOptionsParser;
	import org.apache.hadoop.util.StringUtils;


	public class wordCount {

	    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	        private final static IntWritable one = new IntWritable(1);
	        private Text word = new Text();
	        private Set<String> cache = new HashSet<String>();
			private BufferedReader fis;

			@Override
			public void setup(Context context) throws IOException,
			InterruptedException {
				try {
					fis = new BufferedReader(new FileReader("word-patterns.txt"));
					String patternLine = null;
					while ((patternLine = fis.readLine()) != null) {
						StringTokenizer itr = new StringTokenizer(patternLine.toString());
						while (itr.hasMoreTokens()) {
							cache.add(itr.nextToken());
						}
					}
				} catch (IOException ioe) {
					System.err.println("Caught exception while parsing the cached file '"
							+ StringUtils.stringifyException(ioe));
				}
			}
	        
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	            StringTokenizer itr = new StringTokenizer(value.toString());
	            while (itr.hasMoreTokens()) {
	            	String s = itr.nextToken();
	                if(cache.contains(s)){
	                	word.set(s);
	                	context.write(word, one);
	                	}
	            }
	        }
	    }

	    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	        private IntWritable result = new IntWritable();

	        public void reduce(Text key, Iterable<IntWritable> values, Context context)
	                throws IOException, InterruptedException {
	            int sum = 0;
	            for (IntWritable val : values) {
	                sum += val.get();
	            }
	            result.set(sum);
	            context.write(key, result);
	        }
	    }

	    public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if (otherArgs.length != 2) {
	            System.err.println("Usage: wordcount <in> <out>");
	            System.exit(2);
	        }
			Job job = Job.getInstance(conf, "word count");
	        job.setJarByClass(wordCount.class);
	        job.setMapperClass(TokenizerMapper.class);
	        job.setCombinerClass(IntSumReducer.class);
	        job.setReducerClass(IntSumReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        job.addCacheFile(new URI("/dc/word-patterns.txt#word-patterns.txt"));     
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}
