package com.example.notification.idempotency;

public interface IdempotencyStore {
    /**
     * 처리권 선점. 성공하면 true(처리 진행), 실패하면 false(중복으로 스킵)
     */
    boolean tryAcquire(String key);

    /** 성공 처리 완료 표시 */
    void markDone(String key);

    /** 실패 시 처리권 해제(재시도 가능하게) */
    void release(String key);
}
