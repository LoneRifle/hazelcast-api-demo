package demo.groupby;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamMap;
import demo.Utils;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.KeyExtractors.entryKey;
import static com.hazelcast.jet.Processors.*;

public class GroupSalaryByJet {

    public static void main(String... args) throws Exception {
        JetInstance j = Utils.init();

        IStreamMap<String, Integer> salaries = j.getMap("salaries");
        Utils.fillInSalaries(salaries);

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readMap("salaries"));
        Vertex mapper = dag.newVertex(
            "map-to-digit",
            map((Entry<String, Integer> e) -> {
                String key = e.getKey();
                Integer empId = Integer.valueOf(key.substring(key.length() - 1));
                return new SimpleEntry<>(empId, e.getValue() / 100);
            })
        );
        Vertex accumulator = dag.newVertex(
            "accumulate-by-digit",
            groupAndAccumulate(Entry::getKey, () -> 0,
                    (Integer x, Entry<Integer, Integer> y) -> x + y.getValue())
        );
        Vertex combiner = dag.newVertex(
            "combine-by-digit",
            groupAndAccumulate(Entry::getKey, () -> 0,
                    (Integer x, Entry<Integer, Integer> y) -> x + y.getValue())
        );
        Vertex sink = dag.newVertex("sink", writeMap("counts"));

        dag.edge(between(source, mapper))
           .edge(between(mapper, accumulator).partitioned(entryKey()))
           .edge(between(accumulator, combiner).distributed().partitioned(entryKey()))
           .edge(between(combiner, sink));

        long initialTime = System.currentTimeMillis();
        j.newJob(dag).execute().get();
        Set<?> sum = j.getMap("counts").entrySet();

        long timeTaken = System.currentTimeMillis() - initialTime;

        Hazelcast.shutdownAll();
        System.out.println("Aggregated sum: " + sum + " computed in " + timeTaken + "ms");
    }

}

