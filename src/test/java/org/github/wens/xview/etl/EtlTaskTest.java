package org.github.wens.xview.etl;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class EtlTaskTest {


    @Test
    public void test_1() throws InterruptedException, SQLException {

        String querySql = "select c.course_id ,c.name course_name , sc.student_id ,s.name as student_name from course c left join student_course sc on c.course_id = sc.course_id  left join student s on sc.student_id = s.student_id ";

        String insertSql = "insert into coursex (course_id ,course_name , student_id , student_name ) values (course_id ,course_name , student_id , student_name )" ;

        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUsername("test");
        druidDataSource.setPassword("test@yuyou");
        druidDataSource.setUrl("jdbc:mysql://118.89.27.94:12420/xview?useUnicode=true&characterEncoding=utf8");
        druidDataSource.init();
        System.out.println(druidDataSource.getConnection());
        EtlTask etlTask = new EtlTask(druidDataSource,querySql,insertSql) {

            @Override
            public void beginExtract() {

                System.out.println("beginExtract");
            }

            @Override
            public void extractProgress(int total, int done) {

                System.out.println("extractProgress");
            }

            @Override
            public void completedExtract() {

                System.out.println("completedExtract");
            }

            @Override
            public void extractException(Exception e) {

                System.out.println("extractException");
            }

            @Override
            public void beginTransport() {

                System.out.println("beginTransport");

            }

            @Override
            public void transportProgress(int done) {

                System.out.println("transportProgress");
            }

            @Override
            public void completedTransport() {

                System.out.println("completedTransport");
            }

            @Override
            public void transportException(Exception e) {
                System.out.println("transportException");

            }
        };

        etlTask.start();

        Thread.sleep(TimeUnit.HOURS.toMillis(1l));
    }

}
