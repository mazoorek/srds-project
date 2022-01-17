package cassdemo.scenarios;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

public class SecondScenario extends Thread {
    BackendSession session;

    public SecondScenario(BackendSession session) {
        this.session = session;
    }

    @Override
    public void run() {
        UUID userId = UUID.randomUUID();
        String name = UUID.randomUUID().toString().replace("-", "");
        String password = UUID.randomUUID().toString().replace("-", "");
        String email = UUID.randomUUID().toString().replace("-", "");
        int maxAge = 100;
        int minAge = 18;
        int age = (int) Math.floor(Math.random() * (maxAge - minAge + 1) + minAge);
        try {
            session.createNewUser(userId, name, password, email, age);
            for (int i = 0; i < 20; i++) {
                UUID postId = UUID.randomUUID();
                String categoryName = "category1";
                String postContent = "abc" + i;
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                session.createNewPost(postId, userId, postContent, timestamp, name, categoryName);
                List<Row> posts = session.selectAllPostsByAuthor(userId);

                if(posts.size() !=  i + 1) {
                    System.out.println("ANOMALY");
                }

            }
            System.out.println("-------");
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }
}
