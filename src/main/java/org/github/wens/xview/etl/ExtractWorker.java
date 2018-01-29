package org.github.wens.xview.etl;

import org.github.wens.xview.Lifecycle;
import org.github.wens.xview.XviewException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.concurrent.BlockingQueue;

public class ExtractWorker implements Runnable, Lifecycle {

    private final static Logger log = LoggerFactory.getLogger(ExtractWorker.class);

    private DataSource dataSource;

    private String querySql;

    private BlockingQueue<TableRow> queue;

    private EtlTask etlTask;

    private volatile boolean isStart = false;

    public ExtractWorker(DataSource dataSource, String querySql, BlockingQueue<TableRow> queue, EtlTask etlTask) {

        this.dataSource = dataSource;
        this.querySql = querySql;
        this.queue = queue;
        this.etlTask = etlTask;

    }

    @Override
    public void run() {

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();


            int total = count();
            etlTask.beginExtract();
            statement = connection.prepareStatement(this.querySql);
            ResultSetMetaData metaData = statement.getMetaData();
            resultSet = statement.executeQuery();
            if (resultSet != null) {
                int columnCount = metaData.getColumnCount();
                int c = 0;
                while (resultSet.next()) {
                    c++;
                    TableRow tableRow = new TableRow();
                    for (int i = 1; i <= columnCount; i++) {
                        tableRow.put(metaData.getColumnName(i), resultSet.getObject(i));
                    }
                    try {
                        queue.put(tableRow);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    if (c % 1000 == 0) {
                        etlTask.extractProgress(total, c);
                    }
                }
            }
            etlTask.completedExtract();
        } catch (SQLException e) {
            log.error("Execute query sql fail ", e);
            etlTask.extractException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    //
                }
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    //
                }
            }
        }
        try {
            queue.put(TableRow.EMPTY);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private int count() {
        Connection connection = null ;
        Statement statement = null;
        ResultSet countResult = null;

        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            countResult = statement.executeQuery("select count(*) from (" + this.querySql + ") t ");
            countResult.next();
            return countResult.getInt(1);
        } catch (Exception e) {
            log.error("Execute count sql fail ", e);
            throw new XviewException("Execute count sql fail ", e);
        } finally {

            if (countResult != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    //
                }
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //
                }
            }

            if(connection != null ){
                try {
                    connection.close();
                } catch (SQLException e) {
                    //
                }
            }
        }
    }

    @Override
    public synchronized void start() {
        if(isStart){
            return;
        }
        isStart = true;
        new Thread(this ).start();
    }

    @Override
    public void stop() {

    }
}
