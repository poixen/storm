package com.kaviddiss.storm;

import twitter4j.*;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Custom {@link StatusListener} for the twitter streaming API.
 * Will {@code offer} status' to a {@code LinkedBlockingQueue} for processing later.
 */
public class CustomStatusListener implements StatusListener {
    private final LinkedBlockingQueue<Status> queue;

    public CustomStatusListener(final LinkedBlockingQueue<Status> queue) {
        this.queue = queue;
    }

    @Override
    public void onStatus(Status status) {
        queue.offer(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {

    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }

    @Override
    public void onException(Exception ex) {

    }
}
