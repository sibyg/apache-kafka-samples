package apache_kafka_samples.pojo;

import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class ImmediatePayment {
    private float amount;
    private Account sender;
    private Account receiver;

    public ImmediatePayment() {
    }

    public ImmediatePayment(float amount, Account sender, Account receiver) {
        this.amount = amount;
        this.sender = sender;
        this.receiver = receiver;
    }
}
