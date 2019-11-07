package com.tosin.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据大写
 */
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 拦截Source发送到通道Channel总的消息
     * @param event 接收过滤的Event
     * @return 根据业务处理后的数据
     */
    @Override
    public Event intercept(Event event) {
        byte[] data = event.getBody();
        event.setBody(new String(data).toUpperCase().getBytes());
        return event;
    }

    /**
     * 接收过滤事件集合
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> eventList = new ArrayList<>();
        for (Event event : list) {
            eventList.add(intercept(event));
        }
        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        /**
         * 获取配置文件属性
         * @return
         */
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
