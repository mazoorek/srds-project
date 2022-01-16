package cassdemo;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

	public static String generateRandomAlfabeticString() {
		int leftLimit = 97; // letter 'a'
		int rightLimit = 122; // letter 'z'
		int targetStringLength = 10;
		Random random = new Random();

		String generatedString = random.ints(leftLimit, rightLimit + 1)
				.limit(targetStringLength)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
				.toString();

		return generatedString;
	}

	public static void main(String[] args) throws IOException, BackendException {
		String contactPoint = null;
		String keyspace = null;

		Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
			
		BackendSession session = new BackendSession(contactPoint, keyspace);

		for (int i=0; i < 1; i++) {
			// Create new user
			UUID userId = UUID.randomUUID();
			String name = "Name" + i;
			String password = "Password" + i;
			String email = "email" + i;
			int age = ThreadLocalRandom.current().nextInt(20, 30);
			session.createNewUser(userId, name, password, email, age);



			// Create new post
			UUID postId = UUID.randomUUID();
			String categoryName = "category1";
			String postContent = "abc" + i;
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			session.createNewPost(postId, userId, postContent, timestamp, name, categoryName);

			System.out.println("---------- po authorze ----------");
			String postOutputAuthor1 = session.selectConcretePostByAuthor(userId, timestamp, postId);
			System.out.println(postOutputAuthor1);
			System.out.println("---------- po category ----------");
			String postOutputCategory1 = session.selectConcretePostByCategory(categoryName, timestamp, postId);
			System.out.println(postOutputCategory1);

			UUID commentId = UUID.randomUUID();
			String commentContent = "Komentarz";


			session.createNewComment(postId, userId, name, timestamp, commentId, commentContent);

			System.out.println("DISPLAY COMMENTS: ");

			System.out.println("---------- by POST ID ----------");
			String commentByPostOutput = session.selectCommentsByPost(postId);
			System.out.println(commentByPostOutput);
			System.out.println("---------- po AUTHOR ID ----------");
			String commentByAuthorOutput = session.selectCommentsByAuthor(userId);
			System.out.println(commentByAuthorOutput);

//			//Delete post
//			session.deletePost(postId, userId);
//
			if(i == 49) {
				String allPostsOutput = session.selectAllPostsByAuthor(userId);
				System.out.println(allPostsOutput);
				System.out.println("----------------");
				String allNewestPosts = session.selectNewestPostsByAuthor(userId);
				System.out.println(allNewestPosts);
			}
		}

//		session.upsertUser("PP", "Adam", 609, "A St");
//		session.upsertUser("PP", "Ola", 509, null);
//		session.upsertUser("UAM", "Ewa", 720, "B St");
//		session.upsertUser("PP", "Kasia", 713, "C St");
//
//		String output = session.selectAll();
//		String output2 = session.selectAll();
//		System.out.println("Users: \n" + output);
//
//		session.deleteAll();

		System.exit(0);
	}
}
