package apache_kafka_samples.controller;

import java.util.concurrent.atomic.AtomicLong;

import apache_kafka_samples.pojo.Account;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AccountsController {

    @GetMapping("/account")
    public Account greeting(@RequestParam(value = "sortcode", defaultValue = "00-00-00") String sortCode) {
        return new Account(sortCode, "ACCOUNT-NUMBER", 100F, "GBP");
    }
}
