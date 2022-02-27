import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;



public class Step3 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) {
            try {
                String[] splitted = value.toString().split("\t");
                String _key = splitted[0];
                String val = splitted[1];
                context.write(value,new Text (val) );
            } catch (Exception e) {
                System.out.println(e.toString() + " Step3 map");
            }
        }
    }
/*    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String _key = key.toString();
            String[] words = _key.split("\t");
            return Math.abs(words[0].hashCode() % numPartitions);
        }
    }*/

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try {
                String[] splitted = key.toString().split("\t");
                String _key = splitted[0];
                String val = splitted[1];
                context.write(new Text(_key),new Text(val));
            } catch (Exception e) {
                System.out.println(e.toString() + " Step3 reduce");
            }
        }
    }

    public static class DescendingComperator extends WritableComparator {
        protected DescendingComperator() {
            super(Text.class, true);
        }

        public int compare(WritableComparable line1, WritableComparable line2) {

            String[] splitted1 = line1.toString().split("\t");
            String trigram1 = splitted1[0];
            Double prop1 = Double.parseDouble(splitted1[1]);
            String[] splitted2 = line2.toString().split("\t");
            String trigram2 = splitted2[0];
            Double prop2 = Double.parseDouble(splitted2[1]);
            String[] T1splitted = trigram1.toString().split(" ");
            String[] T2splitted = trigram2.toString().split(" ");
            String w1w21 = T1splitted[0] + T1splitted[1];
            String w1w22 = T2splitted[0] + T2splitted[1];
            if(!w1w21.equalsIgnoreCase(w1w22)){
                return w1w21.compareToIgnoreCase(w1w22) ;
            }
            return prop2.compareTo(prop1);
        }
    }




}
