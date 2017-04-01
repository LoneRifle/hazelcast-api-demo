package demo.sum;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import demo.Utils;

import java.util.Map;

public class SumSalaryByFastAggregation {

    public static void main(String... args) throws Exception {
        HazelcastInstance h = Utils.init().getHazelcastInstance();;
        IMap<String, Integer> salaries = h.getMap("salaries");
        Utils.fillInSalaries(salaries);

        long initialTime = System.currentTimeMillis();
        long sum = salaries.aggregate(new SalaryAggregator());

        long timeTaken = System.currentTimeMillis() - initialTime;

        Hazelcast.shutdownAll();
        System.out.println("Aggregated sum: " + sum + " computed in " + timeTaken + "ms");
    }

}

class SalaryAggregator extends Aggregator<Map.Entry<String, Integer>, Long> {

    private long value = 0;

    @Override
    public void accumulate(Map.Entry<String, Integer> input) {
        value += input.getValue() / 100;
    }

    @Override
    public void combine(Aggregator aggregator) {
        value += ((SalaryAggregator) aggregator).value;
    }

    @Override
    public Long aggregate() {
        return value;
    }
}