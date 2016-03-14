package com.kaviddiss.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Keeps stats on word count, calculates and logs top words every X second
 * to stdout and top list every Y seconds,
 * @author davidk
 */
public class WordCounterBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);

    /** Number of seconds before the top list will be logged to stdout. */
    private final long logIntervalSec;
    /** Number of seconds before the top list will be cleared. */
    private final long clearIntervalSec;
    /** Number of top words to store in stats. */
    private final int topListSize;

    private Map<String, Long> counter;
    private long lastLogTime;
    private long lastClearTime;
    private long timesLogged;
    private long lastWordCountRecieved;
    private long lastAverageWordCount;

    // log every 10 seonds
    // clear every 5 mins
    // max list size is 50
    public WordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
        this.topListSize = topListSize;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        counter = new HashMap<String, Long>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
        timesLogged = 0;
        lastWordCountRecieved = 0;
        lastAverageWordCount = 0;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        Long count = counter.get(word);
        count = count == null ? 1L : count + 1;
        counter.put(word, count);


        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {

            System.out.println();
            logger.info("TOTAL WORD COUNT RECEIVED: " + counter.size());
            logger.info("WORD COUNT RECEIVED: " + (counter.size() - lastWordCountRecieved));

            long averageWordCount = counter.size() / ++timesLogged;
            String averageWordCountStatus;
            if (averageWordCount >= lastAverageWordCount) {
                averageWordCountStatus = "\u2B06";
            } else {
                averageWordCountStatus = "\u2B07";
            }
            logger.info("AVERAGE NEW WORDS PER BATCH: " + averageWordCount + " " + averageWordCountStatus);


            lastLogTime = now;
            lastWordCountRecieved = counter.size();
            lastAverageWordCount = counter.size() / timesLogged;

            publishTopList();
        }
    }


    private void publishTopList() {
        // calculate top list:
        TreeMap<Long, String> top = new TreeMap<Long, String>();

        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            long count = entry.getValue();
            String word = entry.getKey();

            top.put(count, word);
            if (top.size() > topListSize) {
                top.remove(top.firstKey());
            }
        }

        // Output top list:
        int order = 1;
        for (Map.Entry<Long, String> entry : top.descendingMap().entrySet()) {
            logger.info(new StringBuilder(
                    order++ + ". \t\t").append(entry.getValue()).append("\t\t").append(entry.getKey()
            ).toString());
        }

        // Clear top list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
            counter.clear();
            timesLogged = 0;
            lastWordCountRecieved = 0;
            lastClearTime = now;
        }
    }
}
