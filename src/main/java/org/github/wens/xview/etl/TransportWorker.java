package org.github.wens.xview.etl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;
import org.github.wens.xview.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TransportWorker implements Runnable, Lifecycle {

    private Logger logger = LoggerFactory.getLogger(TransportWorker.class);

    private DataSource dataSource;

    private String insertSql;

    private BlockingQueue<TableRow> queue;

    private EtlTask etlTask;

    private volatile boolean isStart = false;

    public TransportWorker(DataSource dataSource, String insertSql, BlockingQueue<TableRow> queue, EtlTask etlTask) {

        this.dataSource = dataSource;
        this.insertSql = insertSql;
        this.queue = queue;
        this.etlTask = etlTask;

    }

    @Override
    public void run() {


        SQLStatementParser parser = new MySqlStatementParser(insertSql);
        SQLInsertStatement sqlInsertStatement = (SQLInsertStatement) parser.parseInsert();
        List<SQLExpr> values = sqlInsertStatement.getValues().getValues();
        List<SQLExpr> newValues = new ArrayList<>(values.size());

        SQLExpr expr = SQLParserUtils.createExprParser("?", JdbcConstants.MYSQL).expr();
        Map<String, Integer> name2index = new HashMap<>();
        for (int i = 0; i < values.size(); i++) {
            SQLExpr oldSqlExpr = values.get(i);
            name2index.put(oldSqlExpr.toString(), i + 1);
            newValues.add(expr);
        }

        this.insertSql = sqlInsertStatement.toString();

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = this.dataSource.getConnection();
            preparedStatement = connection.prepareStatement(this.insertSql);

            etlTask.beginTransport();
            for (long c = 0; ; c++) {
                TableRow tableRow = null;
                try {
                    tableRow = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (tableRow == TableRow.EMPTY) {
                    break;
                }

                PreparedStatement statement = preparedStatement;
                for (Map.Entry<String, Object> entryOfTableRow : tableRow.entrySet()) {
                    statement.setObject(name2index.get(entryOfTableRow.getKey()), entryOfTableRow.getValue());
                }
                statement.addBatch();
                if (c % 1000 == 0) {
                    statement.executeBatch();
                    etlTask.transportProgress((int) c);
                }
            }
            preparedStatement.executeBatch();
            etlTask.completedTransport();
        } catch (Exception e) {
            logger.error("Run transport fail ", e);
            etlTask.transportException(e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
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
