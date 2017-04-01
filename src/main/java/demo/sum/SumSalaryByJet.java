package demo.sum;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.stream.IStreamMap;
import demo.Utils;

public class SumSalaryByJet {

    public static void main(String... args) throws Exception {
        JetInstance j = Utils.init();

        IStreamMap<String, Integer> salaries = j.getMap("salaries");
        Utils.fillInSalaries(salaries);

        long initialTime = System.currentTimeMillis();
        long sum = salaries.stream()
                           .mapToLong(e -> e.getValue().longValue() / 100)
                           .sum();

        long timeTaken = System.currentTimeMillis() - initialTime;

        Hazelcast.shutdownAll();
        System.out.println("Aggregated sum: " + sum + " computed in " + timeTaken + "ms");
    }

}

