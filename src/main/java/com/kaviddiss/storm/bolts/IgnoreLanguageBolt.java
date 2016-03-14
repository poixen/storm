package com.kaviddiss.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Bolt to allow only certain language tweets and emit them
 */
public class IgnoreLanguageBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    private Set<String> ALLOWED_LANGUAGES = new HashSet<String>(Arrays.asList(new String[] {
            "en"
    }));

    @Override
    public void prepare(final Map map, final TopologyContext topologyContext, final OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(final Tuple tuple) {
        Status status = (Status)tuple.getValueByField("tweet");
        String language = status.getUser().getLang();
        if (ALLOWED_LANGUAGES.contains(language)) {
            outputCollector.emit(new Values(status));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}
