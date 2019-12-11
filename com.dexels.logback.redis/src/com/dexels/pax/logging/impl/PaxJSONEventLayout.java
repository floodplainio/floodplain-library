package com.dexels.pax.logging.impl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.ops4j.pax.logging.spi.PaxLocationInfo;
import org.ops4j.pax.logging.spi.PaxLoggingEvent;


/**
 * Adapt from XMLLayout
 * 
 * @author kmtong
 * 
 */
public class PaxJSONEventLayout  {

	private final int DEFAULT_SIZE = 256;
	private final int UPPER_LIMIT = 2048;
	private final static char DBL_QUOTE = '"';
	private final static char COMMA = ',';

	private StringBuilder buf = new StringBuilder(DEFAULT_SIZE);
	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

	private boolean locationInfo = false;
	private int callerStackIdx = 0;
	private boolean properties = false;
	private boolean deDotKeys;

	String source;
	String sourceHost;
	String sourcePath;
	private String sourceInstance;
	private String sourceContainer;
	List<String> tags;
	String type;

	/**
	 * The <b>LocationInfo</b> option takes a boolean value. By default, it is
	 * set to false which means there will be no location information output by
	 * this layout. If the the option is set to true, then the file name and
	 * line number of the statement at the origin of the log statement will be
	 * output.
	 * 
	 * <p>
	 * If you are embedding this layout within an
	 * org.apache.log4j.net.SMTPAppender then make sure to set the
	 * <b>LocationInfo</b> option of that appender as well.
	 */
	public void setLocationInfo(boolean flag) {
		locationInfo = flag;
	}

	/**
	 * Returns the current value of the <b>LocationInfo</b> option.
	 */
	public boolean getLocationInfo() {
		return locationInfo;
	}

	/**
	 * Sets whether MDC key-value pairs should be output, default false.
	 * 
	 * @param flag
	 *            new value.
	 * @since 1.2.15
	 */
	public void setProperties(final boolean flag) {
		properties = flag;
	}

	/**
	 * Gets whether MDC key-value pairs should be output.
	 * 
	 * @return true if MDC key-value pairs are output.
	 * @since 1.2.15
	 */
	public boolean getProperties() {
		return properties;
	}

	/**
	 * Formats a {@link ILoggingEvent} in conformity with the log4j.dtd.
	 */
	@SuppressWarnings("unchecked")
    public synchronized String doLayout(PaxLoggingEvent event) {

		// Reset working buffer. If the buffer is too large, then we need a new
		// one in order to avoid the penalty of creating a large array.
		if (buf.capacity() > UPPER_LIMIT) {
			buf = new StringBuilder(DEFAULT_SIZE);
		} else {
			buf.setLength(0);
		}

		buf.append("{");
		appendKeyValue(buf, "@source", source);
		buf.append(COMMA);
		appendKeyValue(buf, "@source_host", sourceHost);
		buf.append(COMMA);
		appendKeyValue(buf, "@source_path", sourcePath);
		buf.append(COMMA);
		appendKeyValue(buf, "@source_instance", sourceInstance);
		buf.append(COMMA);
		appendKeyValue(buf, "@source_container", sourceContainer);
		buf.append(COMMA);
		appendKeyValue(buf, "@type", type);
		buf.append(COMMA);
		appendKeyValue(buf, "@tags", tags);
		buf.append(COMMA);
		appendKeyValue(buf, "@message", event.getRenderedMessage());
		buf.append(COMMA);
		appendKeyValue(buf, "@timestamp",
				df.format(new Date(event.getTimeStamp())));
		buf.append(COMMA);

		// ---- fields ----
		buf.append("\"@fields\":{");
		appendKeyValue(buf, "logger", event.getLoggerName());
		buf.append(COMMA);
		appendKeyValue(buf, "level", event.getLevel().toString());
		buf.append(COMMA);
		appendKeyValue(buf, "thread", event.getThreadName());
		buf.append(COMMA);
		appendKeyValue(buf, "level", event.getLevel().toString());
		String[] tp = event.getThrowableStrRep();
		if (tp != null && tp.length>0) {
			buf.append(COMMA);
			int i = 0;
			StringBuffer sb = new StringBuffer();
			for (String line : tp) {
				if(i!=0) {
					sb.append("  ");
				}
				sb.append(line);
				sb.append('\n');
				i++;
			}
			String throwable = sb.toString(); // ThrowableProxyUtil.asString(tp);
			appendKeyValue(buf, "throwable", throwable);
		}
		if (locationInfo) {
			PaxLocationInfo callerDataArray = event.getLocationInformation();
			if (callerDataArray != null) {
				buf.append(COMMA);
				buf.append("\"location\":{");
				appendKeyValue(buf, "class", callerDataArray.getClassName());
				buf.append(COMMA);
				appendKeyValue(buf, "method",
						callerDataArray.getMethodName());
				buf.append(COMMA);
				appendKeyValue(buf, "file", callerDataArray.getFileName());
				buf.append(COMMA);
				appendKeyValue(buf, "line",
						callerDataArray.getLineNumber());
				buf.append("}");
			}
		}

		/*
		 * <log4j:properties> <log4j:data name="name" value="value"/>
		 * </log4j:properties>
		 */
		if (properties) {
			Map<String, Object> propertyMap = event.getProperties();
			if ((propertyMap != null) && (propertyMap.size() != 0)) {
				Set<Entry<String, Object>> entrySet = propertyMap.entrySet();
				buf.append(COMMA);
				buf.append("\"properties\":{");
				Iterator<Entry<String, Object>> i = entrySet.iterator();
				while (i.hasNext()) {
					Entry<String, Object> entry = i.next();
					appendKeyValue(buf, deDotKey(entry.getKey()),""+ entry.getValue());
					if (i.hasNext()) {
						buf.append(COMMA);
					}
				}
				buf.append("}");
			}
		}
		buf.append("}");
		buf.append("}");

		return buf.toString();
	}

	private String deDotKey(String key) {
		if(deDotKeys) {
			return key.replaceAll("\\.", "_");
		}
		return key;
		
	}
	private void appendKeyValue(StringBuilder buf, String key, String value) {
		if (value != null) {
			buf.append(DBL_QUOTE);
			buf.append(escape(key));
			buf.append(DBL_QUOTE);
			buf.append(':');
			buf.append(DBL_QUOTE);
			buf.append(escape(value));
			buf.append(DBL_QUOTE);
		} else {
			buf.append(DBL_QUOTE);
			buf.append(escape(key));
			buf.append(DBL_QUOTE);
			buf.append(':');
			buf.append("null");
		}
	}

	private void appendKeyValue(StringBuilder buf, String key,
			List<String> values) {
		buf.append(DBL_QUOTE);
		buf.append(escape(key));
		buf.append(DBL_QUOTE);
		buf.append(':');
		buf.append('[');
		if (values != null) {
			Iterator<String> i = values.iterator();
			while (i.hasNext()) {
				String v = i.next();
				buf.append(DBL_QUOTE);
				buf.append(escape(v));
				buf.append(DBL_QUOTE);
				if (i.hasNext()) {
					buf.append(',');
				}
			}
		}
		buf.append(']');
	}

	private String escape(String s) {
		if (s == null)
			return null;
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			switch (ch) {
			case '"':
				sb.append("\\\"");
				break;
			case '\\':
				sb.append("\\\\");
				break;
			case '\b':
				sb.append("\\b");
				break;
			case '\f':
				sb.append("\\f");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\t':
				sb.append("\\t");
				break;
			case '/':
				sb.append("\\/");
				break;
			default:
				if (ch >= '\u0000' && ch <= '\u001F') {
					String ss = Integer.toHexString(ch);
					sb.append("\\u");
					for (int k = 0; k < 4 - ss.length(); k++) {
						sb.append('0');
					}
					sb.append(ss.toUpperCase());
				} else {
					sb.append(ch);
				}
			}
		}// for
		return sb.toString();
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getSourceHost() {
		return sourceHost;
	}

	public void setSourceHost(String sourceHost) {
		this.sourceHost = sourceHost;
	}

	public String getSourcePath() {
		return sourcePath;
	}

	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setDeDotKeys(boolean flag) {
		this.deDotKeys = flag;
	}
	public String getSourceInstance() {
		return sourceInstance;
	}

	public void setSourceInstance(String sourceContainer) {
		this.sourceContainer = sourceContainer;
	}
	public String getSourceContainer() {
		return sourceContainer;
	}

	public void setSourceContainer(String sourceInstance) {
		this.sourceInstance = sourceInstance;
	}

	
	public int getCallerStackIdx() {
		return callerStackIdx;
	}

	/**
	 * Location information dump with respect to call stack level. Some
	 * framework (Play) wraps the original logging method, and dumping the
	 * location always log the file of the wrapper instead of the actual caller.
	 * For PlayFramework, I use 2.
	 * 
	 * @param callerStackIdx
	 */
	public void setCallerStackIdx(int callerStackIdx) {
		this.callerStackIdx = callerStackIdx;
	}

}
