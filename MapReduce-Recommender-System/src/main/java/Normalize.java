import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input format: movieA:movieB\t relation
            String[] movies_relation = value.toString().trim().split("\t");
            String[] movies = movies_relation[0].trim().split(":");
            String relation = movies_relation[1].trim();

            // target: key=movieA, value=movieB:relation
            context.write(new Text(movies[0]), new Text(movies[1] + ":" + relation));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // input format: key=movieA, value=<movieB:relation, movieC:relation, ...>
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            while (values.iterator().hasNext()) {
                String[] movie_relation = values.toString().trim().split(":");
                String movie = movie_relation[0];
                int relation = Integer.parseInt(movie_relation[1]);
                sum += relation;
                map.put(movie, relation);
            }

            // target: key=movie, value=normalizedRating
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double)entry.getValue()/sum;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
