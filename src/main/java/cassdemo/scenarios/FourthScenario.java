package cassdemo.scenarios;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

public class FourthScenario extends Thread {
    BackendSession session;
    UUID postId;

    public FourthScenario(BackendSession session, UUID postId) {
        this.session = session;
        this.postId = postId;
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
                UUID commentId = UUID.randomUUID();
                String commentContent = "abc" + i;
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                session.createNewComment(postId, userId, name, timestamp, commentId, commentContent);
                List<Row> comments = session.selectCommentsByAuthor(userId);
                if(comments.size() !=  i + 1) {
                    System.out.printf("[%s] expected %d comments, got: %d ANOMALY%n", userId, i + 1, comments.size());
                }
            }
            System.out.println("-------");
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }
}
