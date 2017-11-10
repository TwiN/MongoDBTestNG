import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDriverMongo {
	
	private MongoClient client = new MongoClient("localhost", 32768);
	private MongoDatabase db = client.getDatabase("test");
	private MongoCollection<Document> collection = db.getCollection("test");
	
	
	public void printOutput(FindIterable<Document> output) {
		for (Document doc : output) {
			System.out.println(doc.toJson());
		}
	}
	
	
	@Test
	public void testInsert() {
		collection.drop();
		Document user = new Document("name", "user-01")
			  .append("email", "user-01@example.com")
			  .append("salary", 52000)
			  .append("skills", Collections.singleton("DEV"));
		Document infoUser = new Document("gender", "M")
			  .append("category", 10)
			  .append("currency", "USD");
		
		user.put("infoUser", infoUser);
		collection.insertOne(user);
		
		FindIterable<Document> iter = collection.find(Filters.eq("name", "user-01"));
		
		assertEquals(1, iter.into(new ArrayList<Document>()).size());
		
		// Insert many
		Document user2 = new Document("name", "user-02")
			  .append("email", "user-02@example.com")
			  .append("salary", 45000)
			  .append("skills", Collections.singleton("HR"));
		Document infoUser2 = new Document("gender", "F")
			  .append("category", 15)
			  .append("currency", "CAD");
		
		Document user3 = new Document("name", "user-03")
			  .append("email", "user-03@example.com")
			  .append("salary", 37000)
			  .append("skills", Collections.singleton("SUP"));
		Document infoUser3 = new Document("gender", "M")
			  .append("category", 13)
			  .append("currency", "CAD");
		
		Document user4 = new Document("name", "user-04")
			  .append("email", "user-04@example.com")
			  .append("salary", 10000)
			  .append("skills", Collections.singleton("DEV"));
		Document infoUser4 = new Document("gender", "F")
			  .append("category", 11)
			  .append("currency", "EUR");
		
		user2.put("infoUser", infoUser2);
		user3.put("infoUser", infoUser3);
		user4.put("infoUser", infoUser4);
		
		collection.insertMany(Arrays.asList(user2, user3, user4));
		
		assertEquals(4, collection.count());
	}
	
	
	@Test
	public void testQuerySalary() {
		FindIterable<Document> mongoIter = collection.find(Filters.and(Filters.gte("salary", 30000), Filters.lte("salary", 50000)));
		assertEquals(2, mongoIter.into(new ArrayList<Document>()).size());
		printOutput(mongoIter);
	}
	
	
	@Test
	public void testQueryGender() {
		FindIterable<Document> mongoIter = collection.find(Filters.eq("infoUser.gender", "F"));
		assertEquals(2, mongoIter.into(new ArrayList<Document>()).size());
		printOutput(mongoIter);
	}
	
	
	@Test
	public void testQuerySkills() {
		FindIterable<Document> mongoIter = collection.find(Filters.ne("skills", "DEV"));
		assertEquals(2, mongoIter.into(new ArrayList<Document>()).size());
		printOutput(mongoIter);
	}
	
	
	@Test
	public void testQuery() {
		FindIterable<Document> mongoIter = collection.find(Filters.eq("name", "user-01")).projection(Projections.include("email", "salary"));
		assertEquals(1, mongoIter.into(new ArrayList<Document>()).size());
		printOutput(mongoIter);
	}
	
	
	@Test
	public void testUpdate() {
		collection.updateOne(Filters.eq("name", "user-02"), Updates.set("salary", 1337));
		FindIterable<Document> mongoIter = collection.find(Filters.and(Filters.eq("name", "user-02"), Filters.eq("salary", 1337)));
		assertEquals(1, mongoIter.into(new ArrayList<Document>()).size());
		printOutput(mongoIter);
	}
	
}
