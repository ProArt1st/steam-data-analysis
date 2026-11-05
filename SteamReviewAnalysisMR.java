import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Steam 评测大数据分布式多维归约与词频统计 MapReduce 程序
 * 统计维度:
 *   1. 玩家游玩时长分级区间 (Playtime Tier) 的评测频次、好评率与平均时长
 *   2. 评测主要语言 (Language) 的词频与好评率分布
 */
public class SteamReviewAnalysisMR {

    public static class ReviewMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // TSV 字段: 0:recommendationid, 1:appid, 2:author_steamid, 3:language,
            //          4:is_positive, 5:playtime_hours, 6:votes_up, 7:created_datetime, 8:review_year
            String[] parts = line.split("\t");
            if (parts.length < 6) return;

            String lang = parts[3].trim();
            int isPositive;
            double playtime;
            try {
                isPositive = Integer.parseInt(parts[4].trim());
                playtime = Double.parseDouble(parts[5].trim());
            } catch (NumberFormatException e) {
                return;
            }

            // 确定游玩时长分级桶 (Steam 2小时为无理由退款分水岭)
            String tier;
            if (playtime < 2.0) {
                tier = "TIER_1_Under_2h_(Refund_Window)";
            } else if (playtime < 10.0) {
                tier = "TIER_2_2h_to_10h_(Casual_Play)";
            } else if (playtime < 50.0) {
                tier = "TIER_3_10h_to_50h_(Regular_Play)";
            } else if (playtime < 200.0) {
                tier = "TIER_4_50h_to_200h_(Deep_Play)";
            } else {
                tier = "TIER_5_Over_200h_(Hardcore_Fan)";
            }

            String payload = "1," + isPositive + "," + playtime;
            outVal.set(payload);

            // 输出维度 1: 游玩时长区间统计
            outKey.set(tier);
            context.write(outKey, outVal);

            // 输出维度 2: Top 核心语种统计 (英/简中/俄/西/德/巴葡)
            if (lang.equals("english") || lang.equals("schinese") || lang.equals("russian")
                    || lang.equals("spanish") || lang.equals("german") || lang.equals("brazilian")) {
                outKey.set("LANG_" + lang);
                context.write(outKey, outVal);
            }
        }
    }

    public static class ReviewReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalCount = 0;
            long positiveCount = 0;
            double totalPlaytime = 0.0;

            for (Text val : values) {
                String[] fields = val.toString().split(",");
                if (fields.length == 3) {
                    totalCount += Long.parseLong(fields[0]);
                    positiveCount += Long.parseLong(fields[1]);
                    totalPlaytime += Double.parseDouble(fields[2]);
                }
            }

            if (totalCount > 0) {
                double posRate = (positiveCount * 100.0) / totalCount;
                double avgHours = totalPlaytime / totalCount;
                String summary = String.format("Count=%d\tPositive=%d\tPosRate=%.2f%%\tAvgPlaytime=%.1fh",
                        totalCount, positiveCount, posRate, avgHours);
                result.set(summary);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SteamReviewAnalysisMR <hdfs_input> <hdfs_output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Steam Review Multi-Dimensional MapReduce Analysis");
        job.setJarByClass(SteamReviewAnalysisMR.class);
        job.setMapperClass(ReviewMapper.class);
        job.setReducerClass(ReviewReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
