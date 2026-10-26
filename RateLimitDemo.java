import java.util.concurrent.TimeUnit;

// ---------- Common interface ----------

interface RateLimiter {
    boolean tryAcquire();
}

// ---------- Token bucket ----------
// Tokens refill at a steady rate up to a maximum capacity.
// Each request takes one token. Bursts are allowed up to the capacity.

class TokenBucket implements RateLimiter {
    private final long capacity;        // max tokens the bucket holds
    private final double refillPerNano; // tokens added per nanosecond
    private double tokens;              // current token count
    private long lastRefillNanos;

    TokenBucket(long capacity, double refillPerSecond) {
        this.capacity = capacity;
        this.refillPerNano = refillPerSecond / TimeUnit.SECONDS.toNanos(1);
        this.tokens = capacity;
        this.lastRefillNanos = System.nanoTime();
    }

    @Override
    public synchronized boolean tryAcquire() {
        refill();
        if (tokens >= 1) {
            tokens -= 1;
            return true;
        }
        return false;
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsed = now - lastRefillNanos;
        tokens = Math.min(capacity, tokens + elapsed * refillPerNano);
        lastRefillNanos = now;
    }
}

// ---------- Leaky bucket (meter form) ----------
// Requests fill the bucket. The bucket drains at a constant rate.
// If adding a request would overflow the bucket, it is rejected.
// The output rate is smooth, which is the main difference from the token bucket.

class LeakyBucket implements RateLimiter {
    private final double capacity;      // max volume the bucket holds
    private final double leakPerNano;   // drain rate, in requests per nanosecond
    private double level;               // current volume
    private long lastLeakNanos;

    LeakyBucket(double capacity, double leakPerSecond) {
        this.capacity = capacity;
        this.leakPerNano = leakPerSecond / TimeUnit.SECONDS.toNanos(1);
        this.level = 0;
        this.lastLeakNanos = System.nanoTime();
    }

    @Override
    public synchronized boolean tryAcquire() {
        leak();
        if (level + 1 <= capacity) {
            level += 1;
            return true;
        }
        return false;
    }

    private void leak() {
        long now = System.nanoTime();
        long elapsed = now - lastLeakNanos;
        level = Math.max(0, level - elapsed * leakPerNano);
        lastLeakNanos = now;
    }
}

// ---------- Demo ----------

public class RateLimitDemo {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("== Token bucket: capacity 5, refill 2/sec ==");
        RateLimiter tokenBucket = new TokenBucket(5, 2);
        runBurst(tokenBucket, 8);          // first 5 allowed (burst), rest rejected
        Thread.sleep(1000);                // ~2 tokens refill
        runBurst(tokenBucket, 3);          // about 2 allowed

        System.out.println("\n== Leaky bucket: capacity 5, leak 2/sec ==");
        RateLimiter leakyBucket = new LeakyBucket(5, 2);
        runBurst(leakyBucket, 8);          // first 5 fill the bucket, rest rejected
        Thread.sleep(1000);                // ~2 units drain
        runBurst(leakyBucket, 3);          // about 2 allowed
    }

    private static void runBurst(RateLimiter limiter, int requests) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= requests; i++) {
            sb.append(String.format("req%d=%s ", i, limiter.tryAcquire() ? "ALLOW" : "DENY"));
        }
        System.out.println(sb.toString().trim());
    }
}