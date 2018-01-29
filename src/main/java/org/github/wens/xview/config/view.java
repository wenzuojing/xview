package org.github.wens.xview.config;

public class view {

    public  enum Status {
        INIT,SYNCHRONINZING,COMPLETED
    }

    private String name ;

    private String createSql ;

    private Status status ;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreateSql() {
        return createSql;
    }

    public void setCreateSql(String createSql) {
        this.createSql = createSql;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
