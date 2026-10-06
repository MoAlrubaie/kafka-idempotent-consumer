package io.github.moalrubaie.refunds.demo;

import java.util.Arrays;
import java.util.List;

/**
 * {@code make demo}: one stream of refunds, three sinks, two rounds, one table of results.
 *
 * <pre>
 * --bootstrap host:port    use this Kafka or Redpanda instead of an in-process broker
 * --jdbc-url url           use this Postgres (with --jdbc-user, --jdbc-password) instead of H2
 * --events n               refunds to publish (default 1000)
 * --resend-rate r          share of events the upstream publishes twice (default 0.03)
 * --crash-rate r           share of events that crash once after the payout (default 0.05)
 * --poison n               records that can never be processed (default 4)
 * --seed n                 change it to get a different, but repeatable, run (default 7)
 * --sinks a,b              which sinks to run (default naive,check-then-record,idempotent)
 * </pre>
 */
public final class Demo {

    private Demo() {}

    public static void main(String[] args) throws Exception {
        var options = parse(args);
        long started = System.nanoTime();
        var result = DemoRunner.run(options, line -> System.out.println("  " + line));
        System.out.println();
        System.out.print(Report.render(result, options));
        System.out.printf("%nFinished in %.0f s.%n", (System.nanoTime() - started) / 1e9);
        if (result.sinks().stream()
                .anyMatch(s -> s.sink().equals("idempotent")
                        && !(s.firstPass().exact() && s.replay().exact()))) {
            System.out.println("FAILED: the idempotent sink did not pay every refund exactly once.");
            System.exit(1);
        }
        System.exit(0); // don't wait for Kafka client threads to wind down
    }

    static DemoRunner.Options parse(String[] args) {
        var o = DemoRunner.Options.defaults();
        String bootstrap = o.bootstrap(), url = o.jdbcUrl(), user = o.jdbcUser(), password = o.jdbcPassword();
        int events = o.events(), poison = o.poison();
        double resendRate = o.resendRate(), crashRate = o.crashRate();
        long seed = o.seed();
        List<String> sinks = o.sinks();
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException(flag + " needs a value");
            }
            String value = args[++i];
            switch (flag) {
                case "--bootstrap" -> bootstrap = value;
                case "--jdbc-url" -> url = value;
                case "--jdbc-user" -> user = value;
                case "--jdbc-password" -> password = value;
                case "--events" -> events = Integer.parseInt(value);
                case "--resend-rate" -> resendRate = Double.parseDouble(value);
                case "--crash-rate" -> crashRate = Double.parseDouble(value);
                case "--poison" -> poison = Integer.parseInt(value);
                case "--seed" -> seed = Long.parseLong(value);
                case "--sinks" ->
                    sinks = Arrays.stream(value.split(",")).map(String::trim).toList();
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        for (String sink : sinks) {
            if (!DemoRunner.ALL_SINKS.contains(sink)) {
                throw new IllegalArgumentException(
                        "unknown sink " + sink + ", expected one of " + DemoRunner.ALL_SINKS);
            }
        }
        return new DemoRunner.Options(
                bootstrap, url, user, password, events, resendRate, crashRate, poison, seed, sinks);
    }
}
