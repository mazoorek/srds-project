package cassdemo.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
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

	private static PreparedStatement SELECT_ALL_FROM_USERS;
	private static PreparedStatement INSERT_INTO_USERS;
	private static PreparedStatement DELETE_ALL_FROM_USERS;
	private static PreparedStatement CREATE_NEW_USER;
	private static PreparedStatement CREATE_NEW_POST;
	private static PreparedStatement SELECT_ALL_POSTS;

	private static final String POST_FORMAT = "- %-10s  %-10s %-10s -\n";
	// private static final SimpleDateFormat df = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {
		try {
//			SELECT_ALL_FROM_USERS = session.prepare("SELECT * FROM users;");
//			INSERT_INTO_USERS = session
//					.prepare("INSERT INTO users (companyName, name, phone, street) VALUES (?, ?, ?, ?);");
//			DELETE_ALL_FROM_USERS = session.prepare("TRUNCATE users;");
			SELECT_ALL_POSTS = session.prepare("SELECT * from posts");
			CREATE_NEW_USER = session.prepare("INSERT INTO users (userId, name, surname, age) VALUES (?, ?, ?, ?)");
			CREATE_NEW_POST = session.prepare("INSERT INTO posts (postId, content, authorId) VALUES (?, ?, ?)");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}


	public void upsertUser(String companyName, String name, int phone, String street) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_USERS);
		bs.bind(companyName, name, phone, street);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("User " + name + " upserted");
	}

	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_USERS);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		logger.info("All users deleted");
	}

	public String selectAllPosts() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_POSTS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query: select all posts. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			UUID postId = row.getUUID("postId");
			String content = row.getString("content");
			UUID authorId = row.getUUID("authorId");

			builder.append(String.format(POST_FORMAT, postId, content, authorId));
		}

		return builder.toString();
	}

	public void createNewUser(UUID userId, String name, String surname, int age) throws BackendException {
		BoundStatement bs = new BoundStatement(CREATE_NEW_USER);
		bs.bind(userId, name, surname, age);
		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform insert new user operation. " + e.getMessage() + ".", e);
		}
		logger.info("New user created");
	}

	public void createNewPost(UUID postId, String content, UUID authorId) throws BackendException {
		BoundStatement bs = new BoundStatement(CREATE_NEW_POST);
		bs.bind(postId, content, authorId);
		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform insert new post operation. " + e.getMessage() + ".", e);
		}
		logger.info("New post created");
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
