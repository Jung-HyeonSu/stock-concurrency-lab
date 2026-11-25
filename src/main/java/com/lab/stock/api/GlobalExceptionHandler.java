package com.lab.stock.api;

import com.lab.stock.strategy.StockExceptions.*;
import java.util.Map;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    /** UNIQUE(product_id, user_id) 위반 = 1인 1회 구매 위반. */
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<Map<String, String>> duplicate(DataIntegrityViolationException e) {
        return body(HttpStatus.CONFLICT, "DUPLICATE_ORDER", "이미 주문한 사용자입니다.");
    }

    @ExceptionHandler(ProductNotFoundException.class)
    public ResponseEntity<Map<String, String>> notFound(ProductNotFoundException e) {
        return body(HttpStatus.NOT_FOUND, "PRODUCT_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(UnknownStrategyException.class)
    public ResponseEntity<Map<String, String>> unknownStrategy(UnknownStrategyException e) {
        return body(HttpStatus.BAD_REQUEST, "UNKNOWN_STRATEGY", e.getMessage());
    }

    /** 락 대기/재시도 한도 초과는 서버 과부하 신호이므로 503. */
    @ExceptionHandler({LockAcquisitionException.class, RetryExhaustedException.class,
            PessimisticLockingFailureException.class, StockNotInitializedException.class})
    public ResponseEntity<Map<String, String>> unavailable(RuntimeException e) {
        return body(HttpStatus.SERVICE_UNAVAILABLE, "CONTENTION", e.getMessage());
    }

    private ResponseEntity<Map<String, String>> body(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(Map.of("code", code, "message", String.valueOf(message)));
    }
}
