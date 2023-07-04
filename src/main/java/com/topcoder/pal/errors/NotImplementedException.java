package com.topcoder.pal.errors;

public class NotImplementedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NotImplementedException() {
    }

    public NotImplementedException(String message) {
        super(message);
    }
}