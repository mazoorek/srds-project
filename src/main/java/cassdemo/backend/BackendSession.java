package cassdemo.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;

/*
 * For error handling done right see:
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 *
 * Performing stress tests often results in numerous WriteTimeoutExceptions,
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		List<InetSocketAddress> contactPoints = new ArrayList<>();
		contactPoints.add(new InetSocketAddress(contactPoint, 9042));
		contactPoints.add(new InetSocketAddress(contactPoint, 9043));
		contactPoints.add(new InetSocketAddress(contactPoint, 9044));
		Cluster cluster = Cluster.builder()
				.addContactPoint(contactPoint)
				.addContactPointsWithPorts(contactPoints)
				.withQueryOptions(new QueryOptions().
						setConsistencyLevel(QUORUM))
				.build();

		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement CREATE_NEW_USER;

	private static PreparedStatement CREATE_NEW_POST_AUTHOR;
	private static PreparedStatement CREATE_NEW_POST_CATEGORY;
	private static PreparedStatement SELECT_ALL_POSTS_BY_CATEGORY;
	private static PreparedStatement SELECT_NEWEST_POSTS_BY_CATEGORY;
	private static PreparedStatement SELECT_ALL_POSTS_BY_AUTHOR;
	private static PreparedStatement SELECT_NEWEST_POSTS_BY_AUTHOR;
	private static PreparedStatement DELETE_POST_BY_AUTHOR;
	private static PreparedStatement DELETE_POST_BY_CATEGORY;
	private static PreparedStatement SELECT_CONCRETE_POST_BY_CATEGORY;
	private static PreparedStatement SELECT_CONCRETE_POST_BY_AUTHOR;
	private static PreparedStatement EDIT_CONCRETE_POST_BY_CATEGORY;
	private static PreparedStatement EDIT_CONCRETE_POST_BY_AUTHOR;

	private static PreparedStatement CREATE_NEW_COMMENT_BY_POST;
	private static PreparedStatement CREATE_NEW_COMMENT_BY_AUTHOR;
	private static PreparedStatement SELECT_COMMENTS_BY_POST;
	private static PreparedStatement SELECT_COMMENTS_BY_AUTHOR;
	private static PreparedStatement DELETE_COMMENT_BY_POST;
	private static PreparedStatement DELETE_COMMENT_BY_AUTHOR;
	private static PreparedStatement UPDATE_COMMENT_BY_POST;
	private static PreparedStatement UPDATE_COMMENT_BY_AUTHOR;

	private static PreparedStatement SELECT_POSTS_LIKED_BY_USER;
	private static PreparedStatement CREATE_LIKED_POST_BY_USER;
	private static PreparedStatement DELETE_LIKED_POST_BY_USER;

	private static PreparedStatement SELECT_POST_LIKES;

	private static PreparedStatement INCREMENT_POST_LIKE;
	private static PreparedStatement DECREMENT_POST_LIKE;

	private static PreparedStatement DELETE_POST_LIKES;


	private static final String POST_BY_CATEGORY_FORMAT = "- %-10s %-10s %-10s %-10s %-10s %-10s-\n";
	private static final String POST_BY_AUTHOR_FORMAT = "- %-10s %-10s %-10s %-10s %-10s -\n";
	private static final String COMMENTS_BY_POST_FORMAT = "- %-10s %-10s %-10s %-10s %-10s %-10s -\n";
	private static final String COMMENTS_BY_AUTHOR_FORMAT = "- %-10s %-10s %-10s %-10s %-10s -\n";
	private static final String POST_LIKES_FORMAT = "- %-10s %-10s -\n";
	// private static final SimpleDateFormat df = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_POSTS_BY_CATEGORY = session.prepare("SELECT * from posts_by_category where categoryName = (?)");
			SELECT_NEWEST_POSTS_BY_CATEGORY = session.prepare("SELECT * from posts_by_category where categoryName = (?) LIMIT 10");
			SELECT_ALL_POSTS_BY_AUTHOR = session.prepare("SELECT * from posts_by_author where authorId = (?)").setConsistencyLevel(ONE);
			SELECT_NEWEST_POSTS_BY_AUTHOR = session.prepare("SELECT * from posts_by_author where authorId = (?) LIMIT 10");
			SELECT_CONCRETE_POST_BY_CATEGORY = session.prepare("SELECT * FROM posts_by_category where categoryName = (?) and createdAt = (?) and postId = (?)");
			SELECT_CONCRETE_POST_BY_AUTHOR = session.prepare("SELECT * FROM posts_by_author where authorId = (?) and createdAt = (?) and postId = (?)").setConsistencyLevel(QUORUM);

			CREATE_NEW_USER = session.prepare("INSERT INTO users (userId, name, password, email, age) VALUES (?, ?, ?, ?, ?)");
			CREATE_NEW_POST_AUTHOR = session.prepare("INSERT INTO Posts_by_author (postId, postContent, createdAt, authorId, authorName) VALUES (?, ?, ?, ?, ?)").setConsistencyLevel(ONE);
			CREATE_NEW_POST_CATEGORY = session.prepare("INSERT INTO Posts_by_category (categoryName, postId, postContent, createdAt, authorId, authorName) VALUES (?, ?, ?, ?, ?, ?)").setConsistencyLevel(ONE);

			DELETE_POST_BY_CATEGORY = session.prepare("DELETE FROM posts_by_category where categoryName = (?) and createdAt = (?) and postId = (?)");
			DELETE_POST_BY_AUTHOR = session.prepare("DELETE FROM posts_by_author where authorId = (?) and createdAt = (?) and postId = (?)");

			EDIT_CONCRETE_POST_BY_CATEGORY = session.prepare("UPDATE posts_by_category set postContent = (?) where categoryName = (?) and createdAt = (?) and postId = (?)");
			EDIT_CONCRETE_POST_BY_AUTHOR = session.prepare("UPDATE posts_by_author set postContent = (?) where authorId = (?) and createdAt = (?) and postId = (?)");

			CREATE_NEW_COMMENT_BY_POST = session.prepare("INSERT INTO comments_by_post (postId, authorId, authorName, createdAt, commentId, commentContent) VALUES (?, ?, ?, ?, ?, ?)");
			CREATE_NEW_COMMENT_BY_AUTHOR = session.prepare("INSERT INTO comments_by_author (postId, authorId, createdAt, commentId, commentContent) VALUES (?, ?, ?, ?, ?)");

			SELECT_COMMENTS_BY_POST = session.prepare("SELECT * from comments_by_post where postId = (?)");
			SELECT_COMMENTS_BY_AUTHOR = session.prepare("SELECT * from comments_by_author where authorId = (?)");

			DELETE_COMMENT_BY_POST = session.prepare("DELETE FROM comments_by_post where postId = (?) and createdAt = (?) and commentId = (?)");
			DELETE_COMMENT_BY_AUTHOR = session.prepare("DELETE FROM comments_by_author where authorId = (?) and createdAt = (?) and commentId = (?)");

			UPDATE_COMMENT_BY_POST = session.prepare("UPDATE comments_by_post set commentContent = (?) where postId = (?) and createdAt = (?) and commentId = (?)");
			UPDATE_COMMENT_BY_AUTHOR = session.prepare("UPDATE comments_by_author set commentContent = (?) where authorId = (?) and createdAt = (?) and commentId = (?)");

			SELECT_POSTS_LIKED_BY_USER = session.prepare("SELECT * FROM liked_post_by_user where userId = (?)");
			CREATE_LIKED_POST_BY_USER = session.prepare("INSERT INTO liked_post_by_user (postId, userId) VALUES (?, ?)");
			DELETE_LIKED_POST_BY_USER = session.prepare("DELETE FROM liked_post_by_user where userId = (?) and postId = (?)");

			SELECT_POST_LIKES = session.prepare("SELECT * from post_likes where postId = (?)");
			INCREMENT_POST_LIKE = session.prepare("UPDATE post_likes SET postLikesCounter = postLikesCounter + 1 where postId = (?)");
			DECREMENT_POST_LIKE = session.prepare("UPDATE post_likes SET postLikesCounter = postLikesCounter - 1 where postId = (?)");
			DELETE_POST_LIKES = session.prepare("DELETE FROM post_likes where postId = (?)");

		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}


	public String selectAllPostsByCategory(String categoryName) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_POSTS_BY_CATEGORY);
		bs.bind(categoryName);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all posts. " + e.getMessage() + ".", e);
		}

		showPostsByCategory(rs, builder);

		return builder.toString();
	}

	public List<Row> selectAllPostsByAuthor(UUID authorId) throws BackendException {
//		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(
				SELECT_ALL_POSTS_BY_AUTHOR
		);
		bs.bind(authorId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all posts. " + e.getMessage() + ".", e);
		}

//		showPostsByAuthor(rs, builder);

		return rs.all();
	}

	public String selectNewestPostsByAuthor(UUID authorId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_NEWEST_POSTS_BY_AUTHOR);
		bs.bind(authorId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all posts. " + e.getMessage() + ".", e);
		}

		showPostsByAuthor(rs, builder);

		return builder.toString();
	}

	public String selectNewestPostsByCategory(String categoryName) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_NEWEST_POSTS_BY_CATEGORY);
		bs.bind(categoryName);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all newest posts. " + e.getMessage() + ".", e);
		}

		showPostsByCategory(rs, builder);

		return builder.toString();
	}

	public String selectConcretePostByCategory(String categoryName, Timestamp createdAt, UUID postId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_POST_BY_CATEGORY);
		String temp = "2022-01-15 17:55:45.912000+0000";
//		System.out.println("----------------");
//		System.out.println(createdAt.toString());
		bs.bind(categoryName, createdAt, postId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all newest posts. " + e.getMessage() + ".", e);
		}

		showPostsByCategory(rs, builder);

		return builder.toString();
	}


	public List<Row> selectConcretePostByAuthor(UUID authorId, Timestamp createdAt, UUID postId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_POST_BY_AUTHOR);
		bs.bind(authorId, createdAt, postId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all newest posts. " + e.getMessage() + ".", e);
		}

		return rs.all();
	}

	public void deletePost(UUID postId, UUID authorId, Timestamp createdAt, String categoryName) throws BackendException {
		BoundStatement deletePostByCategoryStatement = new BoundStatement(DELETE_POST_BY_CATEGORY);
		BoundStatement deletePostByAuthorStatement = new BoundStatement(DELETE_POST_BY_AUTHOR);

		deletePostByCategoryStatement.bind(categoryName, createdAt, postId);
		deletePostByAuthorStatement.bind(authorId, createdAt, postId);

		try {
			session.execute(deletePostByCategoryStatement);
			session.execute(deletePostByAuthorStatement);
		} catch (Exception e) {
			throw new BackendException("Could not perform delete post operation. " + e.getMessage() + ".", e);
		}
		logger.info("Post with postId = " + postId + " and authorId = " + authorId + " deleted");
	}

	public void createNewUser(UUID userId, String name, String password, String email, int age) throws BackendException {
		BoundStatement bs = new BoundStatement(CREATE_NEW_USER);
		bs.bind(userId, name, password, email, age);
		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform insert new user operation. " + e.getMessage() + ".", e);
		}
		logger.info("New user created");
	}

	public void createNewPost(UUID postId, UUID authorId, String postContent, Timestamp createdAt, String authorName, String categoryName) throws BackendException {
		BoundStatement bs1 = new BoundStatement(CREATE_NEW_POST_AUTHOR);
		bs1.bind(postId, postContent, createdAt, authorId, authorName);

		BoundStatement bs2 = new BoundStatement(CREATE_NEW_POST_CATEGORY);
		bs2.bind(categoryName, postId, postContent, new Date(), authorId, authorName);
		try {
			session.execute(bs1);
			session.execute(bs2);
		} catch (Exception e) {
			throw new BackendException("Could not perform insert new post operation. " + e.getMessage() + ".", e);
		}
	}

	public void editPost(UUID postId, UUID authorId, String newPostContent, Timestamp createdAt, String categoryName) throws BackendException {
		BoundStatement editPostByCategoryStatement = new BoundStatement(EDIT_CONCRETE_POST_BY_CATEGORY);
		BoundStatement editPostByAuthorStatement = new BoundStatement(EDIT_CONCRETE_POST_BY_AUTHOR);

		editPostByCategoryStatement.bind(newPostContent, categoryName, createdAt, postId);
		editPostByAuthorStatement.bind(newPostContent, authorId, createdAt, postId);

		try {
			session.execute(editPostByCategoryStatement);
			session.execute(editPostByAuthorStatement);
		} catch (Exception e) {
			throw new BackendException("Could not perform insert new post operation. " + e.getMessage() + ".", e);
		}
		logger.info("Post edited");
	}

	public void createNewComment(UUID postId, UUID authorId, String authorName, Timestamp createdAt, UUID commentId, String commentContent) throws BackendException {
		BoundStatement createNewCommentByPostStatement = new BoundStatement(CREATE_NEW_COMMENT_BY_POST);
		BoundStatement createNewCommentByAuthorStatement = new BoundStatement(CREATE_NEW_COMMENT_BY_AUTHOR);

		createNewCommentByPostStatement.bind(postId, authorId, authorName, createdAt, commentId, commentContent);
		createNewCommentByAuthorStatement.bind(postId, authorId, createdAt, commentId, commentContent);

		try {
			session.execute(createNewCommentByPostStatement);
			session.execute(createNewCommentByAuthorStatement);
		} catch (Exception e) {
			throw new BackendException("Could not perform insert new comment operation. " + e.getMessage() + ".", e);
		}
		logger.info("New comment created");
	}

	public String selectCommentsByPost(UUID postId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_COMMENTS_BY_POST);
		bs.bind(postId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all comments by post. " + e.getMessage() + ".", e);
		}

		showCommentsByPost(rs, builder);

		return builder.toString();
	}

	public String selectCommentsByAuthor(UUID authorId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_COMMENTS_BY_AUTHOR);
		bs.bind(authorId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all comments by author. " + e.getMessage() + ".", e);
		}

		showCommentsByAuthor(rs, builder);

		return builder.toString();
	}

	public void deleteComment(UUID postId, Timestamp createdAt, UUID commentId, UUID authorId) throws BackendException {
		BoundStatement deleteCommentByPost = new BoundStatement(DELETE_COMMENT_BY_POST);
		BoundStatement deleteCommentByAuthor = new BoundStatement(DELETE_COMMENT_BY_AUTHOR);

		deleteCommentByPost.bind(postId, createdAt, commentId);
		deleteCommentByAuthor.bind(authorId, createdAt, commentId);

		try {
			session.execute(deleteCommentByPost);
			session.execute(deleteCommentByAuthor);
		} catch (Exception e) {
			throw new BackendException("Could not perform delete comment operation. " + e.getMessage() + ".", e);
		}
		logger.info("Comment with commentId = " + commentId + " and authorId = " + authorId + " and postId = " + postId + " deleted");
	}

	public void editComment(UUID postId, Timestamp createdAt, UUID commentId, UUID authorId, String newCommentContent) throws BackendException {
		BoundStatement editCommentByPost = new BoundStatement(UPDATE_COMMENT_BY_POST);
		BoundStatement editCommentByAuthor = new BoundStatement(UPDATE_COMMENT_BY_AUTHOR);

		editCommentByPost.bind(newCommentContent, postId, createdAt, commentId);
		editCommentByAuthor.bind(newCommentContent, authorId, createdAt, commentId);

		try {
			session.execute(editCommentByPost);
			session.execute(editCommentByAuthor);
		} catch (Exception e) {
			throw new BackendException("Could not perform edit comment operation. " + e.getMessage() + ".", e);
		}
		logger.info("Comment edited");
	}

	public List<UUID> getLikedPostsByUser(UUID userId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_POSTS_LIKED_BY_USER);
		bs.bind(userId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select posts liked by user. " + e.getMessage() + ".", e);
		}

		return rs.all().stream().map(row -> row.getUUID("postId")).collect(Collectors.toList());
	}

	public void createLikedPostByUser(UUID postId, UUID userId) throws BackendException {
		BoundStatement createLikedPostByUserStatement = new BoundStatement(CREATE_LIKED_POST_BY_USER);

		createLikedPostByUserStatement.bind(postId, userId);

		try {
			session.execute(createLikedPostByUserStatement);
		} catch (Exception e) {
			throw new BackendException("Could not perform insert liked post by user operation. " + e.getMessage() + ".", e);
		}
		logger.info("New liked post by user created");
	}

	public void deleteLikedPostByUser(UUID postId, UUID userId) throws BackendException {
		BoundStatement deleteLikedPostByUserStatement = new BoundStatement(DELETE_LIKED_POST_BY_USER);

		deleteLikedPostByUserStatement.bind(userId, postId);

		try {
			session.execute(deleteLikedPostByUserStatement);
		} catch (Exception e) {
			throw new BackendException("Could not perform delete comment operation. " + e.getMessage() + ".", e);
		}
		logger.info("Liked post by user deleted");
	}

	public String selectPostLikes(UUID postId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_POST_LIKES);
		bs.bind(postId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select post likes " + e.getMessage() + ".", e);
		}

		showPostLikes(rs, builder);

		return builder.toString();
	}

	public void incrementPostLikes(UUID postId) throws BackendException {
		BoundStatement incrementPostLikesStatement = new BoundStatement(INCREMENT_POST_LIKE);

		incrementPostLikesStatement.bind(postId);

		try {
			session.execute(incrementPostLikesStatement);
		} catch (Exception e) {
			throw new BackendException("Could not perform increment post likes operation. " + e.getMessage() + ".", e);
		}
		logger.info("Post liked");
	}

	public void decrementPostLikes(UUID postId) throws BackendException {
		BoundStatement decrementPostLikesStatement = new BoundStatement(DECREMENT_POST_LIKE);

		decrementPostLikesStatement.bind(postId);

		try {
			session.execute(decrementPostLikesStatement);
		} catch (Exception e) {
			throw new BackendException("Could not perform decrement post likes operation. " + e.getMessage() + ".", e);
		}
		logger.info("Remove post liked");
	}

	public void deletePostLikes(UUID postId) throws BackendException {
		BoundStatement deletePostLikesStatement = new BoundStatement(DELETE_POST_LIKES);

		deletePostLikesStatement.bind(postId);

		try {
			session.execute(deletePostLikesStatement);
		} catch (Exception e) {
			throw new BackendException("Could not perform delete post likes operation. " + e.getMessage() + ".", e);
		}
		logger.info("post likes deleted");
	}

	private void showPostLikes(ResultSet rs, StringBuilder builder) {
		for (Row row : rs) {
			UUID postId = row.getUUID("postId");
			Long postLikesCounter = row.getLong("postLikesCounter");
			builder.append(String.format(POST_LIKES_FORMAT, postId, postLikesCounter));
		}
	}

	private void showPostsByCategory(ResultSet rs, StringBuilder builder) {
		for (Row row : rs) {
			String category = row.getString("categoryName");
			UUID postId = row.getUUID("postId");
			String postContent = row.getString("postContent");
			Date createdAt = row.getTimestamp("createdAt");
			UUID authorId = row.getUUID("authorId");
			String authorName = row.getString("authorName");
			builder.append(String.format(POST_BY_CATEGORY_FORMAT, category, postId, postContent, createdAt, authorId, authorName));
		}
	}

	private void showPostsByAuthor(ResultSet rs, StringBuilder builder) {
		for (Row row : rs) {
			UUID postId = row.getUUID("postId");
			String postContent = row.getString("postContent");
			Date createdAt = row.getTimestamp("createdAt");
			UUID authorId = row.getUUID("authorId");
			String authorName = row.getString("authorName");
			builder.append(String.format(POST_BY_AUTHOR_FORMAT, postId, postContent, createdAt, authorId, authorName));
		}
	}

	private void showCommentsByPost(ResultSet rs, StringBuilder builder) {
		for (Row row : rs) {
			UUID postId = row.getUUID("postId");
			UUID authorId = row.getUUID("authorId");
			String authorName = row.getString("authorName");
			Date createdAt = row.getTimestamp("createdAt");
			UUID commentId = row.getUUID("commentId");
			String commentContent = row.getString("commentContent");
			builder.append(String.format(COMMENTS_BY_POST_FORMAT, postId, authorId, authorName, createdAt, commentId, commentContent));
		}
	}

	private void showCommentsByAuthor(ResultSet rs, StringBuilder builder) {
		for (Row row : rs) {
			UUID postId = row.getUUID("postId");
			UUID authorId = row.getUUID("authorId");
			Date createdAt = row.getTimestamp("createdAt");
			UUID commentId = row.getUUID("commentId");
			String commentContent = row.getString("commentContent");
			builder.append(String.format(COMMENTS_BY_AUTHOR_FORMAT, postId, authorId, createdAt, commentId, commentContent));
		}
	}



	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
