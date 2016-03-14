package com.kaviddiss.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class WordSplitterBolt extends BaseRichBolt {
    private final int minWordLength;

    private OutputCollector collector;

    public WordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    /**
     * Similar to {@code open} in the {@link backtype.storm.topology.base.BaseRichSpout} class
     * called once to initialize the bolt
     * @param map map containing configuration data
     * @param topologyContext details about the topology
     * @param collector collector for forwarding data
     */
    @Override
    public void prepare(final Map map, final TopologyContext topologyContext, final OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * Emits each word and the language of the user from the passed {@code input}
     * @param input tuple that contains tweet data
     */
    @Override
    public void execute(final Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").toLowerCase();
        String[] words = text.split(" ");
        for (String word : words) {
            if (word.length() >= minWordLength) {
                collector.emit(new Values(lang, word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
