package cassdemo.scenarios;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.Row;

import java.sql.Timestamp;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class FifthScenario extends Thread {
    BackendSession session;

    public FifthScenario(BackendSession session) {
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
        int lastPossibleDecision = 5;
        int firstPossibleDecision = 0;
        int decision;
        try {
            session.createNewUser(userId, name, password, email, age);
            while (true) {
                decision = (int) Math.floor(Math.random() * (lastPossibleDecision - firstPossibleDecision + 1) + firstPossibleDecision);
                switch (decision) {
                    case 0:
                        addPost(userId, name);
                        break;
                    case 1:
                        removePost(userId);
                        break;
                    case 2:
                        addComment(userId, name);
                        break;
                    case 3:
                        removeComment(userId);
                        break;
                    case 4:
                        addLike(userId);
                        break;
                    case 5:
                        removeLike(userId);
                        break;
                    default:
                        addPost(userId, name);
                        break;
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            System.out.println("end of simulation for this thread");
        } catch (BackendException e) {
            System.out.println("session error");
            e.printStackTrace();
        }
    }

    private void addPost(UUID userId, String userName) throws BackendException {
        System.out.printf("[%s] is adding post %n", userId);
        UUID postId = UUID.randomUUID();
        int maxCategory = 3;
        int minCategory = 1;
        String categoryName = "category" + Math.floor(Math.random() * (maxCategory - minCategory + 1) + minCategory);
        String postContent = UUID.randomUUID().toString().replace("-", "");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        session.createNewPost(postId, userId, postContent, timestamp, userName, categoryName);
        System.out.printf("[%s] created post with id=%s %n", userId, postId);
    }

    private void removePost(UUID userId) throws BackendException {
        System.out.printf("> [%s] is removing post %n", userId);
        List<Row> userPosts = session.selectAllPostsByAuthor(userId);
        if (userPosts.size() > 0) {
            Random rand = new Random();
            Row postToRemove = userPosts.get(rand.nextInt(userPosts.size()));
            session.deletePost(
                    postToRemove.getUUID("postId"),
                    userId,
                    new Timestamp(postToRemove.getTimestamp("createdAt").getTime()),
                    postToRemove.getString("categoryName")
            );
            System.out.printf("< [%s] removed post %n", userId);
        } else {
            System.out.printf("[%s] this user don't have any post to remove %n", userId);
        }
    }

    private void addComment(UUID userId, String userName) throws BackendException {
        System.out.printf("> [%s] is adding comment %n", userId);
        int maxCategory = 3;
        int minCategory = 1;
        String categoryName = "category" + Math.floor(Math.random() * (maxCategory - minCategory + 1) + minCategory);
        List<Row> postsByCategory = session.selectAllPostsByCategory(categoryName);
        if (postsByCategory.size() > 0) {
            Random rand = new Random();
            Row postToComment = postsByCategory.get(rand.nextInt(postsByCategory.size()));
            UUID commentId = UUID.randomUUID();
            String commentContent = UUID.randomUUID().toString().replace("-", "");
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            session.createNewComment(postToComment.getUUID("postId"), userId, userName, timestamp, commentId, commentContent);
            System.out.printf("< [%s] added comment %n", userId);
        } else {
            System.out.printf("[%s] this category don't have any posts %n", categoryName);
        }
    }

    private void removeComment(UUID userId) throws BackendException {
        System.out.printf("> [%s] is removing comment %n", userId);
        List<Row> commentsByAuthor = session.selectCommentsByAuthor(userId);
        if (commentsByAuthor.size() > 0) {
            Random rand = new Random();
            Row postToComment = commentsByAuthor.get(rand.nextInt(commentsByAuthor.size()));
            session.deleteComment(
                    postToComment.getUUID("postId"),
                    new Timestamp(postToComment.getTimestamp("createdAt").getTime()),
                    postToComment.getUUID("commentId"),
                    postToComment.getUUID("authorId")
            );
            System.out.printf("< [%s] removed comment %n", userId);
        } else {
            System.out.printf("[%s] this user hasn't commented anything %n", userId);
        }
    }

    private void addLike(UUID userId) throws BackendException {
        System.out.printf("> [%s] is adding a like %n", userId);
        List<Row> posts = session.selectPosts();
        if (posts.size() > 0) {
            Random rand = new Random();
            Row postToLike = posts.get(rand.nextInt(posts.size()));
            if (!session.userLikedPost(userId, postToLike.getUUID("postId"))) {
                session.createLikedPostByUser(postToLike.getUUID("postId"), userId);
                session.incrementPostLikes(postToLike.getUUID("postId"), userId);
                System.out.printf("> [%s] added a like %n", userId);
            } else {
                System.out.printf("> [%s] already added a like %n", userId);
            }
        } else {
            System.out.printf("[%s] there are no comments to like %n", userId);
        }
    }

    private void removeLike(UUID userId) throws BackendException {
        System.out.printf("> [%s] is removing a like %n", userId);
        List<UUID> likes = session.getLikedPostsByUser(userId);
        if (likes.size() > 0) {
            Random rand = new Random();
            UUID postToDislike = likes.get(rand.nextInt(likes.size()));
            session.deletePostLikes(postToDislike);
            session.deleteLikedPostByUser(postToDislike, userId);
            System.out.printf("> [%s] removed a like %n", userId);
        } else {
            System.out.printf("[%s] user hasn't liked anything %n", userId);
        }
    }
}
