import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;

		@Override
		public void setup(Context context) {
			// get N-Gram confirguation from command line
            Configuration configuration = context.getConfiguration();
            noGram = configuration.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			line = line.trim().toLowerCase();

			// remove useless elements?
            line = line.replaceAll("[^a-z]", " ");
			
			// split by ' ', '\t' ...
            String[] words = line.split("\\s+");
            if (words.length < 2) {
                return;
            }
			
			// build N-Gram based on array of words
            StringBuilder stringBuilder;
            for (int i = 0; i < words.length - 1; i++) {
                stringBuilder = new StringBuilder();
                stringBuilder.append(words[i]);
                for (int j = 1; i + j < words.length && j < noGram; j++) {
                    stringBuilder.append(" ");
                    stringBuilder.append(words[i + j]);
                    context.write(new Text(stringBuilder.toString().trim()), new IntWritable(1));
                }
            }
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			// sum up the total count for each N-Gram
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
		}
	}

}