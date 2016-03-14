package com.kaviddiss.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.kaviddiss.storm.bolts.*;
import com.kaviddiss.storm.spouts.TwitterSampleSpout;

/**
 * Topology class that sets up the Storm topology for this sample.
 * Please note that Twitter credentials have to be provided as VM args, otherwise you'll get an Unauthorized error.
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 */
public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-word-count";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);
        // keys can also be passed in via args if needed


        // todo make output for the most popular hashtags coming in from the stream
		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
        b.setBolt("IgnoreLanguageBolt", new IgnoreLanguageBolt()).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("HashTagBolt", new HashTagBolt()).shuffleGrouping("IgnoreLanguageBolt");
		b.setBolt("WordSplitterBolt", new WordSplitterBolt(3)).shuffleGrouping("HashTagBolt");
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("WordSplitterBolt");
        b.setBolt("WordCounterBolt", new WordCounterBolt(10, 60, 20)).shuffleGrouping("IgnoreWordsBolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
