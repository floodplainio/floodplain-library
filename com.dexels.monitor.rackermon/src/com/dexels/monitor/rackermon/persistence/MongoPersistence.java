package com.dexels.monitor.rackermon.persistence;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.Document;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;

import com.dexels.monitor.rackermon.ClusterAlert;
import com.dexels.monitor.rackermon.SLACalculator;
import com.dexels.monitor.rackermon.StateChange;
import com.dexels.monitor.rackermon.checks.api.ServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;

@Component(name = "dexels.monitor.mongopersistence", configurationPolicy = ConfigurationPolicy.REQUIRE, immediate = true, service = {
		MongoPersistence.class, Persistence.class })
public class MongoPersistence implements Persistence {

	MongoDatabase db;
	private MongoClient mongo;

	public MongoPersistence() {
	}

	@Activate
	public void activate(Map<String, Object> settings) {
		mongo = new MongoClient((String) settings.get("host"), 27017);
		db = mongo.getDatabase("monitor");
	}

	@Deactivate
	public void deactivate() {
		db = null;
		if (mongo != null) {
			mongo.close();
		}
	}

	@Override
	public void storeStateChange(StateChange s) {
		try {
			MongoCollection<Document> collection = db.getCollection("statechange", Document.class);
			Document d = new Document();
			d.put("objectid", s.getObjectId());
			d.put("check", s.getCheckString());
			d.put("ts", s.getDate());
			d.put("previousstate", s.getPreviousState());
			d.put("currentstate", s.getCurrentState());
			collection.insertOne(d);
		} catch (Exception e) {
			e.printStackTrace();
			// whateva
		}
	}

	@Override
	public void storeStatus(String objectId, ServerCheckResult status) {
		try {
			MongoCollection<Document> collection = db.getCollection("status", Document.class);
			Document d = new Document();
			d.put("objectid", objectId);
			d.put("ts", new Date());
			d.put("check", status.getCheckName());
			d.put("result", status.getRawResult());
			collection.insertOne(d);
		} catch (Exception e) {
			e.printStackTrace();
			// whateva
		}

	}

	@Override
	public void storeClusterAlert(String name, ServerCheck failed) {
		try {
			MongoCollection<Document> collection = db.getCollection("clusteralert.current", Document.class);
			Document d = new Document();
			d.put("cluster", name);
			d.put("check", failed.getName());
			d.put("startts", new Date());
			collection.insertOne(d);
		} catch (Exception e) {
			e.printStackTrace();
			// whateva
		}
	}

	@Override
	public void storeClusterClearedAlert(String name, ServerCheck checkInFailedCondition) {
		Document query = new Document();
		query.put("cluster", name);
		query.put("check", checkInFailedCondition.getName());
		try {
			MongoCollection<Document> currentcollection = db.getCollection("clusteralert.current", Document.class);
			Document d = currentcollection.findOneAndDelete(query);
			if (d == null) {
				// Wait, why can't we find it? Weird...
			}
			MongoCollection<Document> collection = db.getCollection("clusteralert.history", Document.class);

			d.put("endts", new Date());
			collection.insertOne(d);
		} catch (Exception e) {
			e.printStackTrace();
			// whateva
		}
	}

	@Override
	public List<ClusterAlert> getHistoricClusterAlerts(String name) {
		List<ClusterAlert> result = new ArrayList<>();
		Document query = new Document();
		query.put("cluster", name);
		try {
			MongoCollection<Document> currentcollection = db.getCollection("clusteralert.history", Document.class);
			FindIterable<Document> queryres = currentcollection.find(query);
			if (queryres == null) {
				// Wait, why can't we find it? Weird...
			}
			for (Document d : queryres) {
				String checkName = (String) d.get("check");
				Date dateStart = (Date) d.get("startts");
				Date dateEnd = (Date) d.get("endts");

				// Filter out alerts that lasted less than 1 minute or more than
				// 1 day
				if ((dateEnd.getTime() - dateStart.getTime()) < MIN_ALERT_MS
						|| (dateEnd.getTime() - dateStart.getTime()) > MAX_ALERT_MS) {
					continue;
				}

				ClusterAlert alert = new ClusterAlert(name, checkName, dateStart, dateEnd);
				result.add(alert);
			}

		} catch (Exception e) {
			e.printStackTrace();
			// whateva
		}

		return result;

	}

	@Override
	public String getUptime(String clusterName) {

		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, -1);
		Long minutesMonthDown = getDowntimeInMinutes(clusterName, cal);
		
		cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -6);
        Long minutesSixMontshDown = getDowntimeInMinutes(clusterName, cal);
        
        cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, -1);
        Long minutesYearDown = getDowntimeInMinutes(clusterName, cal);
		
		int totalMinutesInYear = getMinutesFromYear(cal.get(Calendar.YEAR));
		long totalMinutesSixMonths = getMinutesFromYear(cal.get(Calendar.YEAR)) / 2;
		long totalMinutesMonth = getMinutesFromYear(cal.get(Calendar.YEAR)) / 12;

		double uptimeYear = SLACalculator.getUptime(minutesYearDown, totalMinutesInYear);
		double uptimeSixMonths = SLACalculator.getUptime(minutesSixMontshDown, totalMinutesSixMonths);
		double uptimeMonth = SLACalculator.getUptime(minutesMonthDown, totalMinutesMonth);

		return uptimeMonth + " / " + uptimeSixMonths + " / " + uptimeYear;

	}

	/**
	 * 
	 * @param cal The moment from which we should count
	 * @return Returns the number of minutes the cluster had downtime
	 */
    private Long getDowntimeInMinutes(String clusterName, Calendar cal) {
        MongoCollection<Document> dbcoll = db.getCollection("clusteralert.history");
		
		Document matchPipe = new Document();
		matchPipe.put("cluster",  clusterName);
		matchPipe.put("startts",  new Document("$gt", cal.getTime()));
		
		Document projectpipe = new Document();
		List<BsonString> subtract = new ArrayList<>();
		subtract.add(new BsonString("$endts"));
		subtract.add(new BsonString("$startts"));
		projectpipe.put("duration",  new Document("$subtract", new BsonArray(subtract)));
        
        Document match2Pipe = new Document();
        match2Pipe.put("duration",  new Document("$gt", MIN_ALERT_MS).append("$lt", MAX_ALERT_MS));
        
        Document groupPipe = new Document();
        groupPipe.put("_id", "result");
        groupPipe.put("downtime", new Document("$sum", "$duration"));

        List<Document> pipeline = new ArrayList<>();
		pipeline.add(new Document("$match", matchPipe));
		pipeline.add(new Document("$project", projectpipe));
		pipeline.add(new Document("$match", match2Pipe));
		pipeline.add(new Document("$group", groupPipe));
		
		AggregateIterable<Document> aggregate = dbcoll.aggregate(pipeline);
		if (aggregate.first() == null) {
		    return 0L;
		}
        return aggregate.first().getLong("downtime") / 1000 / 60 ;
    }

	private int getMinutesFromYear(int year) {
		return ((year % 400) == 0) ? 366 * 24 * 60 : (year % 100 != 0 && year % 4 == 0) ? 366 * 24 * 60 : 365 * 24 * 60;
	}

}
