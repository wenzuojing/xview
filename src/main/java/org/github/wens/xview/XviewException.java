package org.github.wens.xview;

public class XviewException extends RuntimeException {

    public XviewException() {
    }

    public XviewException(String message) {
        super(message);
    }

    public XviewException(String message, Throwable cause) {
        super(message, cause);
    }

    public XviewException(Throwable cause) {
        super(cause);
    }

    public XviewException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
