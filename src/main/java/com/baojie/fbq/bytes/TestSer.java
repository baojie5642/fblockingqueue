package com.baojie.fbq.bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TestSer.class);

    private final String info;
    private final long id;


    public TestSer(String info, long id) {
        this.info = info;
        this.id = id;
    }

    @Override
    public void run() {
        log.error("suc, info=" + info + ", id=" + id);
    }

}
