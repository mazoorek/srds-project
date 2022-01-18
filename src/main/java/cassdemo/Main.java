package cassdemo;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.scenarios.*;

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
			System.out.println("3: 100 users like one post expect seeing correct amount of likes:");
			System.out.println("4: 50 users commenting the same post:");
			System.out.println("5: 50 users blog simulation:");
			System.out.println("6: clear tables:");
			scenario = sc.nextInt();
			if(scenario == 0) {
				break;
			} else if (scenario == 1) {
				scenarioService.execute(new FirstScenario(session), 50);
			} else if (scenario == 2) {
				scenarioService.execute(new SecondScenario(session), 50);
			} else if (scenario == 3) {
				// Create new user
				UUID userId = UUID.randomUUID();
				String name = UUID.randomUUID().toString().replace("-", "");
				String password = UUID.randomUUID().toString().replace("-", "");
				String email = UUID.randomUUID().toString().replace("-", "");
				int maxAge = 100;
				int minAge = 18;
				int age = (int) Math.floor(Math.random() * (maxAge - minAge + 1) + minAge);
				session.createNewUser(userId, name, password, email, age);
				// Create new post
				UUID postId = UUID.randomUUID();
				System.out.println("---- POST ID: " + postId + " -----");
				String categoryName = "counterTestCategory";
				String postContent = UUID.randomUUID().toString().replace("-", "");
				Timestamp timestamp = new Timestamp(System.currentTimeMillis());
				session.createNewPost(postId, userId, postContent, timestamp, name, categoryName);
				scenarioService.execute(new ThirdScenario(session, postId, timestamp, userId), 100);
			} else if(scenario == 4) {
				// Create new user
				UUID userId = UUID.randomUUID();
				String name = UUID.randomUUID().toString().replace("-", "");
				String password = UUID.randomUUID().toString().replace("-", "");
				String email = UUID.randomUUID().toString().replace("-", "");
				int maxAge = 100;
				int minAge = 18;
				int age = (int) Math.floor(Math.random() * (maxAge - minAge + 1) + minAge);
				session.createNewUser(userId, name, password, email, age);
				// Create new post
				UUID postId = UUID.randomUUID();
				System.out.println("---- POST ID: " + postId + " -----");
				String categoryName = "counterTestCategory";
				String postContent = UUID.randomUUID().toString().replace("-", "");
				Timestamp timestamp = new Timestamp(System.currentTimeMillis());
				session.createNewPost(postId, userId, postContent, timestamp, name, categoryName);
				scenarioService.execute(new FourthScenario(session, postId), 100);
			} else if(scenario == 5) {
				scenarioService.executeForTimeInSeconds(new FifthScenario(session), 50, 10);
			} else if(scenario == 6) {
				session.truncateTables();
			}
		}
		System.exit(0);
	}
}
