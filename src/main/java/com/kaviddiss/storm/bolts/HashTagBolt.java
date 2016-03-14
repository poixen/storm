package com.kaviddiss.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by matthewlowe on 07/01/16.
 */
public class HashTagBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(HashTagBolt.class);


    private OutputCollector outputCollector;

    private Set<String> HASH_TAGS = new HashSet<String>(Arrays.asList(new String[] {
       "MTG", "mtg"
    }));

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        for (HashtagEntity entity : tweet.getHashtagEntities()) {
            if (HASH_TAGS.contains(entity.getText())) {
                logger.info("TWEET: " + tweet.getText());
                outputCollector.emit(new Values(tweet));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}
