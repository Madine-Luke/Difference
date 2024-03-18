import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class Difference {
    public static class DifferenceMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString());
            String row = "";

            if (scanner.hasNextLine()) {
                row = scanner.nextLine();
            }

            String[] data = row.split(",");
            String date = "";
            String dateKey = "";
            String remain = "";
            String[] tem = {};

            for (int i = 0; i < data.length; i++) {
                if (i == 0) {
                    date = data[i];
                }

                String[] dates = date.split("-");
                tem = dates;

                if (dates[0].compareTo("2021") >= 0 && dates[0].compareTo("2022") <= 0) {

                    if (i > 2) {
                        remain += "," + data[i];
                    }

                }

                dateKey = dates[1];
            }

            context.write(new Text(dateKey), new Text(tem[0] + remain));
        }
    }

    public static class DifferenceReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String dateKey = key.toString();
            int[] doses1 = new int[12];
            int[] doses2 = new int[12];
            int sum1 = 0;
            int sum2 = 0;
            String out = "";

            for (Text val : values) {
                String[] data = val.toString().split(",");
                for (int i = 1; i < data.length; i++) {
                    if (data[0].compareTo("2021") == 0) {
                        doses1[i - 1] += Integer.parseInt(data[i].trim());
                    } else if (data[0].compareTo("2022") == 0) {
                        doses2[i - 1] += Integer.parseInt(data[i].trim());
                    }
                }
            }
            for (int i = 0; i < doses1.length; i++) {
                sum1 += doses1[i];
                sum2 += doses2[i];
            }
            int dif = sum2 - sum1;
            out = "," + dif;
            String kvString = dateKey + out;
            if (dateKey.compareTo("01") != 0 && dateKey.compareTo("02") != 0) {
                context.write(new Text(kvString), new Text());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: Difference <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Difference by Jinchuan");
        job.setJarByClass(Difference.class);

        job.setMapperClass(DifferenceMapper.class);
        job.setReducerClass(DifferenceReducer.class);
        job.setOutputKeyClass(Text.class); //output key
        job.setOutputValueClass(Text.class); //output value

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
