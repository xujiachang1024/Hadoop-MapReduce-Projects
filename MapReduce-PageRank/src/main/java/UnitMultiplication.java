import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // input format: fromPage\t toPage1,toPage2,toPage3
            String[] fromPage_toPages = value.toString().trim().split("\t");
            if (fromPage_toPages.length < 2 || fromPage_toPages[2].trim().equals("")) {
                return;
            }

            // target: build transition matrix unit -> <fromPage, toPage=probability>
            String fromPage = fromPage_toPages[0];
            String[] toPages = fromPage_toPages[1].split(",");
            for (String toPage : toPages) {
                context.write(new Text(fromPage), new Text(toPage + "=" + (double)1/toPages.length));
            }

        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: page\t pageRank
            String[] pageRank = value.toString().trim().split("\t");

            //target: <page, pageRank>
            context.write(new Text(pageRank[0]), new Text(pageRank[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        float beta;

        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            beta = configuration.getFloat("beta", 0.2f);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability, ..., pageRank>
            List<String> transactionUnits = new ArrayList<String>();
            double pageRankUnit = 0;
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transactionUnits.add(value.toString());
                } else {
                    pageRankUnit = Double.parseDouble(value.toString());
                }
            }

            //target: key=toPage, value=unitMultiplication
            for (String transactionUnit : transactionUnits) {
                String outputKey = transactionUnit.split("=")[0];
                double relation = Double.parseDouble(transactionUnit.split("=")[1]);
                String outputValue = String.valueOf(relation * pageRankUnit * (1-beta));
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        // chain two mapper classes
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
