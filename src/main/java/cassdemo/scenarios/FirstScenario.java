package cassdemo.scenarios;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

public class FirstScenario extends Thread {
    BackendSession session;

    public FirstScenario(BackendSession session) {
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
                session.createNewPost(postId, userId, postContent, timestamp, name, categoryName, ConsistencyLevel.QUORUM);
                List<Row> posts = session.selectConcretePostByAuthor(userId, timestamp, postId, ConsistencyLevel.QUORUM);

                if(posts.size() == 0) {
                    System.out.printf("[%s] expected post with id:%s, iteration: %d %n", userId, postId,i);
                }

            }
            System.out.println("-------");
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }
}
