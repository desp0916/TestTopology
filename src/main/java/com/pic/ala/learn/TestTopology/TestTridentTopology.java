package com.pic.ala.learn.TestTopology;

import java.util.HashMap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

public class TestTridentTopology extends TridentTopology {

	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;
	private final static String configKey = "trident-config";

	public TestTridentTopology() {

		FixedBatchSpout spout = new FixedBatchSpout(
				new Fields("sentence"), 3,
				new Values("the cow jumped over the moon"),
				new Values("the man went to the store and bought some candy"),
				new Values("four score and seven years ago"),
				new Values("how many apples can you eat"));
		spout.setCycle(true);

		TridentState wordCounts = this.newStream("spout1", spout)
				.each(new Fields("sentence"), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
				.parallelismHint(6);

		this.newDRPCStream("words")
				.each(new Fields("args"), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
				.each(new Fields("count"), new FilterNull())
				.aggregate(new Fields("count"), new Sum(), new Fields("sum"));
	}

	public void runLocal(TestTridentTopology topology, int runTime) {
		conf.setDebug(true);

		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		if (runTime > 0) {
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}

	public void runCluster(TestTridentTopology topology, String name)
			throws AlreadyAliveException, InvalidTopologyException,
			AuthorizationException {
		// conf.setDebug(true);
		conf.setNumWorkers(5);

		HashMap<String, Object> clientConfig = new HashMap<String, Object>();
		conf.put(configKey, clientConfig);

		StormSubmitter.submitTopology(name, conf, topology.build());
	}

	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	public static void main(String args[]) throws Exception {
		TestTridentTopology topology = new TestTridentTopology();
		if (args != null && args.length > 1) {
			topology.runCluster(topology, args[0]);
		} else {
			if (args != null && args.length == 1) {
				System.out
						.println("Running in local mode, redis ip missing for cluster run");
			}
			topology.runLocal(topology, 10000);
		}

	}
}
