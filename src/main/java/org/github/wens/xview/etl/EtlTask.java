package org.github.wens.xview.etl;

import org.github.wens.xview.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class EtlTask implements Lifecycle {

    private final Logger log = LoggerFactory.getLogger(EtlTask.class);


    private BlockingQueue<TableRow> queue;

    private ExtractWorker extractWorker;

    private TransportWorker transportWorker;

    private volatile boolean isStart = false;

    public EtlTask(DataSource dataSource, String querySql, String insertSql) {
        this.queue = new LinkedBlockingQueue<>(500000);
        this.extractWorker = new ExtractWorker(dataSource, querySql, this.queue, this);
        this.transportWorker = new TransportWorker(dataSource, insertSql, this.queue, this);
    }

    public abstract void beginExtract();

    public abstract void extractProgress(int total, int done);

    public abstract void completedExtract();

    public abstract void extractException(Exception e);

    public abstract void beginTransport();

    public abstract void transportProgress(int done);

    public abstract void completedTransport();

    public abstract void transportException(Exception e);

    @Override
    public synchronized void start() {

        if (isStart) {
        }

        extractWorker.start();
        transportWorker.start();

    }

    @Override
    public void stop() {
        extractWorker.stop();
        transportWorker.stop();
    }

}
