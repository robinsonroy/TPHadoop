import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.*;

public class IntArrayWritable extends ArrayWritable{

    public IntArrayWritable() {

        super(IntWritable.class);
    }


    public IntArrayWritable(IntWritable[] data) {
        super(IntWritable.class, data);
    }
}