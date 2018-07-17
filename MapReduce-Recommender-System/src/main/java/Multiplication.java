import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input format: movieB\t movieA=relation
            String[] line = value.toString().trim().split("\t");

			// target: key=movieB, value=movieA=relation
            context.write(new Text(line[0]), new Text(line[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input format: user,movie,rating
            String[] user_movie_rating = value.toString().trim().split(",");
            String user = user_movie_rating[0];
            String movie = user_movie_rating[1];
            String rating = user_movie_rating[2];

			// target: key=movie, value=user:rating
            context.write(new Text(movie), new Text(user + ":" + rating));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// input format key=movieB value=<movieA=relation, movieC=relation... userA:rating, userB:rating...>
            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> ratingMap = new HashMap<String, Double>();

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    String[] movie_relation = value.toString().split("=");
                    String movie = movie_relation[0];
                    double relation = Double.parseDouble(movie_relation[1]);
                    relationMap.put(movie, relation);
                } else {
                    String[] user_rating = value.toString().split(":");
                    String user = user_rating[0];
                    double rating = Double.parseDouble(user_rating[1]);
                    ratingMap.put(user, rating);
                }
            }

			// target: key=user:movie, value=relation*rating
            for (Map.Entry<String, Double> relationEntry : relationMap.entrySet()) {
                String movie = relationEntry.getKey();
                double relation = relationEntry.getValue();
                for (Map.Entry<String, Double> ratingEntry : ratingMap.entrySet()) {
                    String user = ratingEntry.getKey();
                    double rating = ratingEntry.getValue();
                    context.write(new Text(user + ":" + movie), new DoubleWritable(relation * rating));
                }
            }
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
