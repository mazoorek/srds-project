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
		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).addContactPointsWithPorts(contactPoints).build();

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

	private static final String POST_BY_CATEGORY_FORMAT = "- %-10s %-10s %-10s %-10s %-10s %-10s-\n";
	private static final String POST_BY_AUTHOR_FORMAT = "- %-10s %-10s %-10s %-10s %-10s -\n";
	// private static final SimpleDateFormat df = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_POSTS_BY_CATEGORY = session.prepare("SELECT * from posts_by_category where categoryName = (?)");
			SELECT_NEWEST_POSTS_BY_CATEGORY = session.prepare("SELECT * from posts_by_category where categoryName = (?) LIMIT 10");
			SELECT_ALL_POSTS_BY_AUTHOR = session.prepare("SELECT * from posts_by_author where authorId = (?)");
			SELECT_NEWEST_POSTS_BY_AUTHOR = session.prepare("SELECT * from posts_by_author where authorId = (?) LIMIT 10");
			SELECT_CONCRETE_POST_BY_CATEGORY = session.prepare("SELECT * FROM posts_by_category where categoryName = (?) and createdAt = (?) and postId = (?)");
			SELECT_CONCRETE_POST_BY_AUTHOR = session.prepare("SELECT * FROM posts_by_author where authorId = (?) and createdAt = (?) and postId = (?)");

			CREATE_NEW_USER = session.prepare("INSERT INTO users (userId, name, password, email, age) VALUES (?, ?, ?, ?, ?)");
			CREATE_NEW_POST_AUTHOR = session.prepare("INSERT INTO Posts_by_author (postId, postContent, createdAt, authorId, authorName) VALUES (?, ?, ?, ?, ?)");
			CREATE_NEW_POST_CATEGORY = session.prepare("INSERT INTO Posts_by_category (categoryName, postId, postContent, createdAt, authorId, authorName) VALUES (?, ?, ?, ?, ?, ?)");

			DELETE_POST_BY_CATEGORY = session.prepare("DELETE FROM posts_by_category where categoryName = (?) and createdAt = (?) and postId = (?)");
			DELETE_POST_BY_AUTHOR = session.prepare("DELETE FROM posts_by_author where authorId = (?) and createdAt = (?) and postId = (?)");

			EDIT_CONCRETE_POST_BY_CATEGORY = session.prepare("UPDATE posts_by_category set postContent = (?) where categoryName = (?) and createdAt = (?) and postId = (?)");
			EDIT_CONCRETE_POST_BY_AUTHOR = session.prepare("UPDATE posts_by_author set postContent = (?) where authorId = (?) and createdAt = (?) and postId = (?)");

			CREATE_NEW_COMMENT_BY_POST = session.prepare("INSERT INTO comments_by_post (postId, authorId, authorName, createdAt, commentId, commentContent) VALUES (?, ?, ?, ?, ?, ?)");
			CREATE_NEW_COMMENT_BY_AUTHOR = session.prepare("INSERT INTO comments_by_author (postId, authorId, createdAt, commentId, commentContent) VALUES (?, ?, ?, ?, ?)");



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

	public String selectAllPostsByAuthor(UUID authorId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_POSTS_BY_AUTHOR);
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


	public String selectConcretePostByAuthor(UUID authorId, Timestamp createdAt, UUID postId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_POST_BY_AUTHOR);
		bs.bind(authorId, createdAt, postId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all newest posts. " + e.getMessage() + ".", e);
		}

		showPostsByAuthor(rs, builder);

		return builder.toString();
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
		logger.info("New post created");
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
