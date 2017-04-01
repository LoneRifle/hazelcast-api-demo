package demo.groupby;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import demo.Utils;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class GroupSalaryByDistributedExecutor {

    public static void main(String... args) throws Exception {
        HazelcastInstance h = Utils.init().getHazelcastInstance();
        IMap<String, Integer> salaries = h.getMap("salaries");
        Utils.fillInSalaries(salaries);

        long initialTime = System.currentTimeMillis();
        Map<Integer, List<Future<?>>> digitToTasks = new HashMap<>();

        for (int i = 0; i < 1000; ++i) {
            List<Future<?>> tasks = digitToTasks.computeIfAbsent(i % 10, ArrayList::new);

            tasks.add(
                salaries.submitToKey("Employee-" + i, new EntryProcessor<String, Integer>() {
                    @Override
                    public Object process(Entry<String, Integer> entry) {
                        return entry.getValue() / 100;
                    }

                    @Override
                    public EntryBackupProcessor<String, Integer> getBackupProcessor() {
                        return null;
                    }
                })
            );
        }

        Map<Integer, Integer> digitToSummedSalaries = digitToTasks.entrySet().stream().map(e ->
            new SimpleEntry<>(
                e.getKey(),
                e.getValue().stream().reduce(0, (s, f) -> s + (Integer)getFromFutureOrBlowup(f), (x, y) -> x + y)
            )
        ).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        long timeTaken = System.currentTimeMillis() - initialTime;

        Hazelcast.shutdownAll();
        System.out.println("Aggregated sum: " + digitToSummedSalaries + " computed in " + timeTaken + "ms");
    }

    private static <T> T getFromFutureOrBlowup(Future<T> f) {
        try {
            return f.get();
        } catch (ExecutionException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

}
