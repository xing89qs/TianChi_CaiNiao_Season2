package my.group.GradientBoostingRegression;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Combiner模板。请用真实逻辑替换模板内容
 */
public class FeatureCombiner implements Reducer {
    private Record count;

    public void setup(TaskContext context) throws IOException {
        count = context.createMapOutputValueRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        long c = 0;
        while (values.hasNext()) {
            Record val = values.next();
            c += val.getBigint(0);
        }
        count.set(0, c);
        context.write(key, count);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
