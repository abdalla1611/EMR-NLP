import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step2 {
    static int WordCounter = 163471963*3;
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) {
            try {
                String[] splitted = value.toString().split("\t");
                Text word = new Text(splitted[0]);
                Text val = new Text(splitted[1] + '\t' + splitted[2] + '\t' + splitted[3] + '\t' + splitted[4] + '\t' + splitted[5] + '\t' + splitted[6]);
                context.write(word, val);
            } catch (Exception e) {
                System.out.println(e.toString() + " Step2 map");
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try {
                Long[] params = {0L, 0L, 0L, 0L, 0L, 0L};  // n1,n2,n3 ...
                for (Text val : values) {
                    String[] CurrentParams = val.toString().split("\t");
                    if (CurrentParams.length == 6) {
                        int idx = 0;
                        for (String param : CurrentParams) {
                            try {
                                params[idx] += Integer.parseInt(param);
                            } catch (Exception e) {
                                params[idx] = 0L;
                                System.out.println("there is a non number element in the array, key = " + key.toString());
                            }
                            idx++;
                        }
                    } else {
                        System.out.println("CurrentParams length is not 6! it is " + CurrentParams.length + " and->" + val.toString());
                    }
                }
                if(params[4] == 0 || params[5] == 0) {
                    System.out.println(key.toString());
                    return;
                }
                double k2 = Math.log(params[1] + 1) / Math.log(params[1] + 2);
                double k3 = Math.log(params[2] + 1) / Math.log(params[2] + 2);
                double prob =
                        k3 * ((float) params[2] / params[5])
                                + (1 - k3) * k2 * ((float) params[1] / params[4])
                                + (1 - k3) * (1 - k2) * params[0] / WordCounter;
                Text probability = new Text("" + prob);
                context.write(key, probability);
            } catch (Exception e) {
                System.out.println(e.toString() + " Step2 reduce");
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String _key = key.toString();
            String[] words = _key.split("\t");
            return Math.abs(words[0].hashCode() % numPartitions);
        }
    }
}
