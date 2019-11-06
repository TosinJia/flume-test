package com.tosin.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] data = event.getBody();
        event.setBody(new String(data).toUpperCase().getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> eventList = new ArrayList<>();
        for (Event event : list) {
            eventList.add(event);
        }
        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
