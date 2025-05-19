import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text hashtag = new Text();
    private static final Pattern HASHTAG_PATTERN = Pattern.compile("#\\w+");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = HASHTAG_PATTERN.matcher(value.toString());
        while (matcher.find()) {
            hashtag.set(matcher.group());
            context.write(hashtag, one);
        }
    }
}
