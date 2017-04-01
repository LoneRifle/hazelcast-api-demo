package demo.sum;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import demo.Utils;

import java.util.Map;

public class SumSalaryByMapReduce {

    public static void main(String... args) throws Exception {
        HazelcastInstance h = Utils.init().getHazelcastInstance();
        IMap<String, Integer> salaries = h.getMap("salaries");
        Utils.fillInSalaries(salaries);

        long initialTime = System.currentTimeMillis();
        JobTracker jobTracker = h.getJobTracker("salaryTracker");
        KeyValueSource<String, Integer> source = KeyValueSource.fromMap(salaries);

        Job<String, Integer> job = jobTracker.newJob(source);

        ICompletableFuture<Integer> future = job
                .mapper(new SalaryMapper())
                .reducer(new SalaryReducerFactory())
                .submit(new SalaryCollator());

        Integer sum = future.get();
        long timeTaken = System.currentTimeMillis() - initialTime;

        Hazelcast.shutdownAll();
        System.out.println("Aggregated sum: " + sum + " computed in " + timeTaken + "ms");
    }

}

class SalaryCollator
        implements Collator<Map.Entry<String, Integer>, Integer> {

    @Override
    public Integer collate(Iterable<Map.Entry<String, Integer>> values) {
        int value = 0;
        for (Map.Entry<String, Integer> entry : values) {
            value += entry.getValue();
        }
        return value;
    }
}

class SalaryMapper
        implements Mapper<String, Integer, String, Integer> {

    @Override
    public void map(String key, Integer value, Context<String, Integer> context) {
        context.emit("salarysum", value / 100);
    }
}


class SalaryReducerFactory
        implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String key) {
        return new SalaryReducer();
    }

    private static class SalaryReducer
            extends Reducer<Integer, Integer> {

        private volatile int value = 0;

        @Override
        public void reduce(Integer value) {
            this.value += value;
        }

        @Override
        public Integer finalizeReduce() {
            return value;
        }
    }
}

