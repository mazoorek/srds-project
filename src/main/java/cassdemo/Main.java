package cassdemo;

import java.io.IOException;
import java.util.*;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.scenarios.FirstScenario;
import cassdemo.scenarios.SecondScenario;

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

	public static void main(String[] args) throws IOException, BackendException, InterruptedException {
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
		ScenarioService scenarioService = new ScenarioService();

		Scanner sc= new Scanner(System.in);    //System.in is a standard input stream
		System.out.print("Enter first number- ");
		int scenario;
		while(true) {
			System.out.println("Choose scenario:");
			System.out.println("0: Exit program");
			System.out.println("1: 50 users adding posts expect seeing added by primary key:");
			System.out.println("2: 50 users adding posts expect seeing added by authorId:");
			scenario = sc.nextInt();
			if(scenario == 0) {
				break;
			} else if (scenario == 1) {
				scenarioService.execute(new FirstScenario(session), 50);
			} else if (scenario == 2) {
				scenarioService.execute(new SecondScenario(session), 50);
			}
		}

//		for (int i=0; i < 1; i++) {
//			// Create new user
//			UUID userId = UUID.randomUUID();
//			String name = "Name" + i;
//			String password = "Password" + i;
//			String email = "email" + i;
//			int age = ThreadLocalRandom.current().nextInt(20, 30);
//			session.createNewUser(userId, name, password, email, age);
//
//
//
//			// Create new post
//			UUID postId = UUID.randomUUID();
//			String categoryName = "category1";
//			String postContent = "abc" + i;
//			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//			session.createNewPost(postId, userId, postContent, timestamp, name, categoryName);
//
//			System.out.println("---------- po authorze ----------");
//			String postOutputAuthor1 = session.selectConcretePostByAuthor(userId, timestamp, postId);
//			System.out.println(postOutputAuthor1);
//			System.out.println("---------- po category ----------");
//			String postOutputCategory1 = session.selectConcretePostByCategory(categoryName, timestamp, postId);
//			System.out.println(postOutputCategory1);
//
//			System.out.println("Liked post by user before create liked");
//			List<UUID> likedPost = session.getLikedPostsByUser(userId);
//			likedPost.forEach(System.out::println);
//			System.out.println("--------------");
//
//			session.createLikedPostByUser(postId, userId);
//
//			System.out.println("Liked post by user after create liked");
//			likedPost = session.getLikedPostsByUser(userId);
//			likedPost.forEach(System.out::println);
//
//			System.out.println("Delete like");
//			session.deleteLikedPostByUser(postId, userId);
//			System.out.println("-------------------------");
//
//			System.out.println("Liked post by user after delete liked");
//			likedPost = session.getLikedPostsByUser(userId);
//			likedPost.forEach(System.out::println);
//
//			System.out.println("DISPLAY POST LIKES");
//			String postLikes = session.selectPostLikes(postId);
//			System.out.println(postLikes);
//
//			System.out.println("INCREMENT POST LIKES");
//			session.incrementPostLikes(postId);
//
//			System.out.println("DISPLAY AFTER INCREMENT");
//			String postLikes2 = session.selectPostLikes(postId);
//			System.out.println(postLikes2);
//
//			System.out.println("DECREMENT POST LIKES");
//			session.decrementPostLikes(postId);
//
//			System.out.println("DISPLAY AFTER DECREMENT");
//			String postLikes3 = session.selectPostLikes(postId);
//			System.out.println(postLikes3);
//
//			System.out.println("DELETE POST LIKES");
//			session.deletePostLikes(postId);
//
//			System.out.println("POST LIKES AFTER DELETE");
//			String postLikes4 = session.selectPostLikes(postId);
//			System.out.println(postLikes4);
//
////
//			if(i == 49) {
//				String allPostsOutput = session.selectAllPostsByAuthor(userId);
//				System.out.println(allPostsOutput);
//				System.out.println("----------------");
//				String allNewestPosts = session.selectNewestPostsByAuthor(userId);
//				System.out.println(allNewestPosts);
//			}
//		}

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
