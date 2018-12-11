package com.adrienben.demo;

public class RetryableException extends RuntimeException {
	public RetryableException(String message) {
		super(message);
	}
}
