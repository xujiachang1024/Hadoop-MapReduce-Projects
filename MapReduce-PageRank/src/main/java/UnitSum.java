import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // input format: toPage\t unitMultiplication
            String[] toPage_unitMultiplication = value.toString().split("\t");
            String toPage = toPage_unitMultiplication[0];
            double unitMultiplication = Double.parseDouble(toPage_unitMultiplication[1]);

            // target: pass to reducer
            context.write(new Text(toPage), new DoubleWritable(unitMultiplication));
        }
    }

    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        float beta;

        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            beta = configuration.getFloat("beat", 0.2f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // input format: toPage\t pageRank
            String[] toPage_pageRank = value.toString().split("\t");

            // target: key=toPage, value=pageRank*beta
            String toPage = toPage_pageRank[0];
            double betaRank = Double.parseDouble(toPage_pageRank[1]) * beta;
            context.write(new Text(toPage), new DoubleWritable(betaRank));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            // input: key = toPage, value = <unitMultiplication>
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }

            // target: sum
            DecimalFormat decimalFormat = new DecimalFormat("#.0000");
            sum = Double.valueOf(decimalFormat.format(sum));
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);
        job.setMapperClass(PassMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
