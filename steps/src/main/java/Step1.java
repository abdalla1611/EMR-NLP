import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step1 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        int i = 0;

        @Override
        public void map(LongWritable key, Text value, Context context) {
            for (int i = 0; i < value.toString().length(); i++) {
                if (value.toString().charAt(i) == '!')
                    return;
            }
            try {
                String[] line = value.toString().split("\t");
                String[] words = line[0].split(" ");
                if (words.length == 3) {
                    String years = line[1]; // I don't know if i need this !.
                    String occur = line[2];
                    String correct3 = "=" + words[0] + ' ' + words[1] + ' ' + words[2]; //w1w2w3
                    context.write(new Text(words[0] + " ! !"), new Text(occur));
                    context.write(new Text(words[0] + ' ' + words[1] + " !"), new Text(occur));
                    context.write(new Text(words[0] + ' ' + words[1] + ' ' + words[2] + correct3), new Text(occur));
                    context.write(new Text(words[1] + " ! !"), new Text(occur));
                    context.write(new Text(words[1] + ' ' + words[2] + " !"), new Text(occur));
                    context.write(new Text(words[1] + ' ' + words[2] + ' ' + words[0] + correct3), new Text(occur));
                    context.write(new Text(words[2] + " ! !"), new Text(occur));
                    context.write(new Text(words[2] + ' ' + words[1] + ' ' + words[0] + correct3), new Text(occur));
                } else {
                    System.out.println("Wrong input, input = " + value.toString());
                }
            } catch (Exception e) {
                System.out.println(e.toString() + " Step1 map");
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        int n1 = 0;
        int n2 = 0;
        int n3 = 0;
        int c1 = 0;
        int c2 = 0;
        int tmp1 = 0;
        int tmp2 = 0;
        int tmp3 = 0;
        String WDoubleStar = "";
        String w2w3 = "";
        String w1w2 = "";
        long C0 = 0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) {
            String word = key.toString();
            try {
                if (word.contains("! !")) {
                    tmp1 = 0;
                    tmp2 = 0;
                    for (Text i : values) {
                        tmp1 += Integer.parseInt(i.toString());
                    }
                    WDoubleStar = word.split(" ")[0];
                } else if (word.contains("!")) {
                    tmp2 = 0;
                    w1w2 = word.split(" ")[0] + ' ' + word.split(" ")[1]; //w1w2
                    w2w3 = w1w2;
                    for (Text i : values) {
                        tmp2 += Integer.parseInt(i.toString());
                    }
                } else if (word.contains("=")) {
                    tmp3 = 0;
                    String FalseWord = word.split("=")[0];
                    word = word.split("=")[1];
                    String currw2w3 = word.split(" ")[1] + ' ' + word.split(" ")[2];
                    String currw1w2 = word.split(" ")[0] + ' ' + word.split(" ")[1];
                    String w3 = word.split(" ")[2];
                    String w2 = word.split(" ")[1];
                    String w1 = word.split(" ")[0];
                    for (Text i : values) {
                        tmp3 += Integer.parseInt(i.toString());
                    }
                    if (word.equals(FalseWord))
                        n3 = tmp3;
                    if (WDoubleStar.equals(w1))
                        C0 = tmp1;
                    if (WDoubleStar.equals(w2))
                        c1 = tmp1;
                    if (WDoubleStar.equals(w3))
                        n1 = tmp1;
                    if (w2w3.equals(currw2w3))
                        n2 = tmp2;
                    if (w1w2.equals(currw1w2))
                        c2 = tmp2;

                    // now we have all the available information about the current word,
                    // we need to put this information in a file or something and send it to the next step
                    Text outputWord = new Text(word);
                    String s = n1 + "\t" + n2 + '\t' + n3 + '\t' + C0 + '\t' + c1 + '\t' + c2;
                    Text value = new Text(s);
                    context.write(outputWord, value);
                    n1 = 0;
                    n2 = 0;
                    n3 = 0;
                    c1 = 0;
                    c2 = 0;
                } else {
                    System.out.println("Something is wrong");
                }
            } catch (Exception e) {
                System.out.println(e.toString() + " Step1 reduce");
            }
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            int count = 0;
            try {
                for (Text txt : values) {
                    count += Integer.parseInt(txt.toString());
                }
                context.write(key, new Text(String.valueOf(count)));
            } catch (Exception e) {
                System.out.println(e.toString() + " Step1 combiner");
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            try {
                String _key = key.toString();
                String[] words = _key.split(" ");
                return Math.abs(words[0].hashCode() % numPartitions);
            } catch (Exception e) {
                System.out.println(e.toString() + " partitioner");
            }
            return -1;
        }
    }
}