package com.dexels.navajo.adapters.stream;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.davidmoten.rx.jdbc.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.navajo.document.Operand;
import com.dexels.navajo.document.Property;

import io.reactivex.Flowable;

public class SQL {
	
	private SQL() {
		// no instances
	}
	
	private static final Logger logger = LoggerFactory.getLogger(SQL.class);

	private static DataSource testDataSource = null;

	
	public static Optional<DataSource> resolveDataSource(String dataSourceName, String tenant) {
		
		return Optional.empty();
	}
	
	public static Flowable<ImmutableMessage> query(String datasource, String tenant, String query, Operand... params) {
		Optional<DataSource> ds = resolveDataSource(datasource, tenant);
		if(!ds.isPresent()) {
			return Flowable.error(new NullPointerException("Datasource missing for datasource: "+datasource+" with tenant: "+tenant));
		}
		List<Object> valueList = Arrays.asList(params).stream().map(e->e.value).collect(Collectors.toList());
		return Database.fromBlocking(ds.get())
			.select(query)
			.parameters(valueList)
			.get(SQL::resultSet);
	}
	
	public static Flowable<ImmutableMessage> update(String datasource, String tenant, String query, Operand... params) {
		Optional<DataSource> ds = resolveDataSource(datasource, tenant);
		if(!ds.isPresent()) {
			return Flowable.error(new NullPointerException("Datasource missing for datasource: "+datasource+" with tenant: "+tenant));
		}
		List<Object> valueList = Arrays.asList(params).stream().map(e->e.value).collect(Collectors.toList());
		return Database.fromBlocking(ds.get())
			.update(query)
			.parameters(valueList)
			.counts()
			.map(count->ImmutableFactory.empty().with("count", count, Property.INTEGER_PROPERTY));
	}
	
	private static ImmutableMessage toLowerCaseKeys(ImmutableMessage m) {
		Set<String> names = m.columnNames();
		Map<String,Object> values = new HashMap<>();
		Map<String,String> types = new HashMap<>();
		names.forEach(e->{
			values.put(e.toLowerCase(), m.value(e).orElse(null));
			types.put(e.toLowerCase(), m.columnType(e));
		});
		return ImmutableFactory.create(values, types, Collections.emptyMap(), Collections.emptyMap());
	}

	public static ImmutableMessage defaultSqlResultToMsg(SQLResult result) {
		return result.toMessage();
	}
	
	
	
	public static ImmutableMessage resultSet(ResultSet rs)  {
			try {
				return toLowerCaseKeys(new SQLResult(rs)
						.toMessage());
			} catch (Exception e) {
				logger.error("Error: ", e);
			}
			return null;
	}
}