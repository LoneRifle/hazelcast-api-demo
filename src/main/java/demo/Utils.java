package demo;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;

public class Utils {

    public static JetInstance init() {
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig();
        Jet.newJetInstance(jetConfig);
        Jet.newJetInstance(jetConfig);
        return Jet.newJetInstance(jetConfig);
    }

    public static void fillInSalaries(IMap<String, Integer> salaries) {
        for (int i = 0; i < 1000; ++i) {
            salaries.put("Employee-" + i, i * 10);
        }
    }
}
