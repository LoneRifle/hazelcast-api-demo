package demo.sum;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import demo.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
public class SumSalaryByDistributedExecutor {

    public static void main(String... args) throws Exception {
        HazelcastInstance h = Utils.init().getHazelcastInstance();
        IMap<String, Integer> salaries = h.getMap("salaries");
        Utils.fillInSalaries(salaries);

        long initialTime = System.currentTimeMillis();
        List<Future<?>> tasks = new ArrayList<>(1000);

        for (int i = 0; i < 1000; ++i) {
            tasks.add(
                salaries.submitToKey("Employee-" + i, new EntryProcessor<String, Integer>() {
                    @Override
                    public Object process(Map.Entry<String, Integer> entry) {
                        return entry.getValue() / 100;
                    }

                    @Override
                    public EntryBackupProcessor<String, Integer> getBackupProcessor() {
                        return null;
                    }
                })
            );
        }

        int sum = 0;
        for (Future<?> t : tasks) {
            sum += (Integer) t.get();
        }
        long timeTaken = System.currentTimeMillis() - initialTime;

        Hazelcast.shutdownAll();
        System.out.println("Aggregated sum: " + sum + " computed in " + timeTaken + "ms");
    }

}
