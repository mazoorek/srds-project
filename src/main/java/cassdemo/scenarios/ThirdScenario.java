package cassdemo.scenarios;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

public class ThirdScenario extends Thread {
    BackendSession session;
    UUID postId;
    Timestamp createdAt;
    UUID authorId;

    public ThirdScenario(BackendSession session, UUID postId, Timestamp createdAt, UUID authorId) {
        this.session = session;
        this.postId = postId;
        this.createdAt = createdAt;
        this.authorId = authorId;
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

            List<Row> posts = session.selectConcretePostByAuthor(authorId, createdAt, postId);

            if(posts.size() != 0) {
                session.incrementPostLikes(postId, userId);
            }
            System.out.println("-------");
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }
}
