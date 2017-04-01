package demo.groupby;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import demo.Utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GroupSalaryByFastAggregation {

    public static void main(String... args) throws Exception {
        HazelcastInstance h = Utils.init().getHazelcastInstance();;
        IMap<String, Integer> salaries = h.getMap("salaries");
        Utils.fillInSalaries(salaries);

        long initialTime = System.currentTimeMillis();
        Map<Integer, Integer> sum = salaries.aggregate(new SalaryAggregator());

        long timeTaken = System.currentTimeMillis() - initialTime;

        Hazelcast.shutdownAll();
        System.out.println("Aggregated sum: " + sum + " computed in " + timeTaken + "ms");
    }

}

class SalaryAggregator extends Aggregator<Map.Entry<String, Integer>, Map<Integer, Integer>> {

    private ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<>();

    @Override
    public void accumulate(Map.Entry<String, Integer> input) {
        String key = input.getKey();
        Integer empId = Integer.valueOf(key.substring(key.length() - 1));
        map.merge(empId, input.getValue() / 100, (i, v) -> i + v);
    }

    @Override
    public void combine(Aggregator aggregator) {
        Map<Integer, Integer> m = ((SalaryAggregator) aggregator).aggregate();
        m.forEach((k, v) -> map.merge(k, v, (i, j) -> i + j));
    }

    @Override
    public Map<Integer, Integer> aggregate() {
        return map;
    }
}