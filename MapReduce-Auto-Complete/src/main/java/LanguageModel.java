import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			// get the threshold parameter from the configuration
			Configuration configuration = context.getConfiguration();
			threshold = configuration.getInt("threshold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(value == null || value.toString().trim().length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			// filter the n-gram lower than threshold
            if (count < threshold) {
                return;
            }
			
			// this is --> cool = 20
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                stringBuilder.append(words[i]);
                stringBuilder.append(" ");
            }
            String outputKey = stringBuilder.toString().trim();
            String outputValue = words[words.length - 1];
			
			// write key-value to reducer
            if (outputKey != null || outputKey != "") {
                context.write(new Text(outputKey), new Text(outputValue));
            }
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;

		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			// this is, <girl = 50, boy = 60>
            TreeMap<Integer, List<String>> treeMap = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
            for (Text value : values) {
                String word = value.toString().trim().split("=")[0].trim();
                int count = Integer.parseInt(value.toString().trim().split("=")[1].trim());
                if (treeMap.containsKey(count)) {
                    treeMap.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    treeMap.put(count, list);
                }
            }

            // <50, <girl, bird>> <60, <boy...>>
            Iterator<Integer> iterator = treeMap.keySet().iterator();
            for (int i = 0; iterator.hasNext() && i < n; i++) {
                int count = iterator.next();
                List<String> words = treeMap.get(count);
                for (String word : words) {
                    context.write(new DBOutputWritable(key.toString(), word, count), NullWritable.get());
                    i++;
                }
            }
		}
	}
}
