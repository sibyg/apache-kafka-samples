package apache_kafka_samples.pojo;

import lombok.Data;

@Data
public class Account {
    private String sortCode;
    private String accountNumber;
    private float balance;
    private String currency;

    public Account() {
    }

    public Account(String sortCode, String accountNumber, float balance, String currency) {
        this.sortCode = sortCode;
        this.accountNumber = accountNumber;
        this.balance = balance;
        this.currency = currency;
    }
}
