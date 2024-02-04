

package org.example.exception;


public class TransportException extends RuntimeException {
    /**
     * 异常状态码
     */
    private String code;

    /**
     * 异常信息
     */
    private String msg;

    public TransportException(String message) {
        super(message);
        this.msg = message;
    }

    public TransportException(String code, String message) {
        super(message);
        this.code = code;
        this.msg = message;
    }
}
