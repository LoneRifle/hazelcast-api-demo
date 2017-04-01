package demo.groupby;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.*;
import demo.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GroupSalaryByMapReduce {

    public static void main(String... args) throws Exception {
        HazelcastInstance h = Utils.init().getHazelcastInstance();
        IMap<String, Integer> salaries = h.getMap("salaries");
        Utils.fillInSalaries(salaries);

        long initialTime = System.currentTimeMillis();
        JobTracker jobTracker = h.getJobTracker("salaryTracker");
        KeyValueSource<String, Integer> source = KeyValueSource.fromMap(salaries);

        Job<String, Integer> job = jobTracker.newJob(source);

        ICompletableFuture<Map<Integer, Integer>> future = job
                .mapper(new SalaryMapper())
                .reducer(new SalaryReducerFactory())
                .submit(new SalaryCollator());

        Map<Integer, Integer> sum = future.get();
        long timeTaken = System.currentTimeMillis() - initialTime;

        Hazelcast.shutdownAll();
        System.out.println("Aggregated sum: " + sum + " computed in " + timeTaken + "ms");
    }

}

class SalaryCollator
        implements Collator<Entry<Integer, Integer>, Map<Integer, Integer>> {

    @Override
    public Map<Integer, Integer> collate(Iterable<Entry<Integer, Integer>> values) {
        Map<Integer, Integer> map = new HashMap<>();
        for (Entry<Integer, Integer> e : values) {
            map.put(e.getKey(), e.getValue());
        }
        return map;
    }
}

class SalaryMapper
        implements Mapper<String, Integer, Integer, Integer> {

    @Override
    public void map(String key, Integer value, Context<Integer, Integer> context) {
        String empId = key.substring(key.length() - 1);
        context.emit(Integer.valueOf(empId), value / 100);
    }
}


class SalaryReducerFactory
        implements ReducerFactory<Integer, Integer, Integer> {

    private ConcurrentMap<Integer, SalaryReducer> map = new ConcurrentHashMap<>();

    @Override
    public Reducer<Integer, Integer> newReducer(Integer key) {
        return map.computeIfAbsent(key, s -> new SalaryReducer());
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

