package org.davidmoten.rx.jdbc;

public class MemoryUsageMain {

    public static void main(String[] args) throws InterruptedException {
        Database db = Database.test(1);
        for (int i = 0; i < 1000; i += 1) {
            db.select("select count(*) from person").get(m -> {
                try {
                    Thread.sleep(1000000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return 1;
            }).forEach(System.out::println);
        }
        Thread.sleep(1000);
        System.gc();
        Thread.sleep(500000000L);
    }

}
