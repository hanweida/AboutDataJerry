package com.kafka.consumer.task;
import com.google.common.primitives.Bytes;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.io.UnsupportedEncodingException;

public class ConsumerMsgTask implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerMsgTask(KafkaStream stream, int threadNumber) {
        m_threadNumber = threadNumber;
        m_stream = stream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext())
            try {
                System.out.println("Thread " + m_threadNumber + ": "
                        + new String(it.next().message() ,"gbk"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}