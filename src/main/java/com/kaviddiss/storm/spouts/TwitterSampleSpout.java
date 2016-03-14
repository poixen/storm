/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/
 */
package com.kaviddiss.storm.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.kaviddiss.storm.CustomStatusListener;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * @author davidk
 */
@SuppressWarnings({"rawtypes", "serial"})
public class TwitterSampleSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

    /**
     * Called once to initialize the spout.
     *
     * @param conf any configuration data for the topology
     * @param context context of the topology
     * @param collector data collecter to forward data to the next bolt
     */
    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        this.collector = collector;

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true);
        configurationBuilder.setOAuthAccessToken(conf.get("access_token").toString());
        configurationBuilder.setOAuthAccessTokenSecret(conf.get("access_token_secret").toString());
        configurationBuilder.setOAuthConsumerKey(conf.get("consumer_key").toString());
        configurationBuilder.setOAuthConsumerSecret(conf.get("consumer_secret").toString());

        TwitterStreamFactory factory = new TwitterStreamFactory(configurationBuilder.build());
        twitterStream = factory.getInstance();
        twitterStream.addListener(new CustomStatusListener(queue));
        twitterStream.sample(); // starts to sample the stream on another thread
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}
