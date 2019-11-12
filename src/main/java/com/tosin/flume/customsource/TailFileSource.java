package com.tosin.flume.customsource;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 *  断点续传
 *  自定义source，记录偏移量
 *  flume的生命周期： 先执行构造器，再执行 config方法 -> start方法-> processor.process
 *  读取配置文件:(配置读取的文件内容：读取个文件，编码及、偏移量写到那个文件，多长时间检测一下文件是否有新内容
 *
 */
public class TailFileSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ExecSource.class);
    private String filePath;
    private String charset;
    private String positionFile;
    private long interval;

    private ExecutorService executor;

    private FileRunnable fileRunnable;

    /**
     * 读取配置文件（flume在执行一次job时定义的配置文件）
     * (如果在flume的job的配置文件中不修改，就是用这些默认的配置)
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        //读取哪个文件
        filePath = context.getString("filePath");
        //默认使用utf-8
        charset = context.getString("charset", "UTF-8");
        //把偏移量写到哪
        positionFile = context.getString("positionFile");
        //指定默认每个一秒 去查看一次是否有新的内容
        interval = context.getLong("interval", 1000L);
    }


    /**
     * 创建一个线程来监听一个文件
     */
    @Override
    public synchronized void start() {
        //创建一个单线程的线程池
        executor = Executors.newSingleThreadExecutor();
        //获取一个ChannelProcessor
        final ChannelProcessor channelProcessor = getChannelProcessor();

        fileRunnable = new FileRunnable(filePath, charset, positionFile, interval, channelProcessor);
        //提交到线程池中
        executor.submit(fileRunnable);
        //调用父类的方法
        super.start();


    }

    @Override
    public synchronized void stop() {
        //停止
        fileRunnable.setFlag(false);
        //停止线程池
        executor.shutdown();

        while (!executor.isTerminated()) {
            logger.debug("Waiting for filer exec executor service to stop");
            try {
                //等500秒在停
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("InterutedExecption while waiting for exec executor service" +
                        " to stop . Just exiting");
                e.printStackTrace();
            }
        }
        super.stop();
    }


    private static class FileRunnable implements Runnable {


        private String charset;

        private long interval;
        private long offset = 0L;
        private ChannelProcessor channelProcessor;
        private RandomAccessFile raf;
        private boolean flag = true;
        private File posFile;

        /*
        先于run方法执行，构造器只执行一次
        先看看有没有偏移量，如果有就接着读，如果没有就从头开始读
         */
        public FileRunnable(String filePath, String charset, String positionFile, long interval, ChannelProcessor channelProcessor) {


            this.charset = charset;
            this.interval = interval;
            this.channelProcessor = channelProcessor;
            //读取偏移量， 在postionFile文件
            posFile = new File(positionFile);
            if (!posFile.exists()) {
                //如果不存在就创建一个文件
                try {
                    posFile.createNewFile();
                } catch (IOException e) {

                    e.printStackTrace();
                    logger.error("创建保存偏移量的文件失败:", e);
                }
            }

            try {
                //读取文件的偏移量
                String offsetString = FileUtils.readFileToString(posFile);

                //以前读取过
                if (!offsetString.isEmpty() && null != offsetString && !"".equals(offsetString)) {
                    //把偏移量穿换成long类型
                    offset = Long.parseLong(offsetString);
                }

                //按照指定的偏移量读取数据
                raf = new RandomAccessFile(filePath, "r");
                //按照指定的偏移量读取
                raf.seek(offset);
            } catch (IOException e) {
                logger.error("读取保存偏移量文件时发生错误", e);
                e.printStackTrace();
            }


        }

        @Override
        public void run() {

            while (flag) {
                //读取文件中的新数据
                try {
                    String line = raf.readLine();

                    if (line != null) {
                        //有数据进行处理，避免出现乱码
                        line = new String(line.getBytes("iso8859-1"), charset);
                        channelProcessor.processEvent(EventBuilder.withBody(line.getBytes()));
                        //获取偏移量,更新偏移量
                        offset = raf.getFilePointer();
                        //将偏移量写入到位置文件中
                        FileUtils.writeStringToFile(posFile, offset + "");
                    } else {
                        //没读到睡一会儿
                        Thread.sleep(interval);
                    }
                    //发给channle
                    //更新偏移量


                    //每个时间间隔读取一次

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error("read filethread Interrupted", e);
                } catch (IOException e) {
                    logger.error("read log file error", e);
                }
            }


        }

        public void setFlag(boolean flag) {
            this.flag = flag;
        }
    }
}