import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.*;

public class IntArrayWritable extends ArrayWritable{

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(int[] data) {
        super(IntWritable.class);

        IntWritable[] tmp = new IntWritable[data.length];
        for (int i = 0; i < data.length; ++i)
            tmp[i] = new IntWritable(data[i]);

        this.set(tmp);
    }

    public IntArrayWritable(IntWritable[] data) {
        super(IntWritable.class, data);
    }
}