import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.NavajoFactory;
import com.dexels.navajo.document.stream.StreamDocument;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestTMLReplicationConversion {

	@Test
	public void testReplToMessage() {
		ReplicationMessage rm = ReplicationFactory.empty().with("Monkey", "Koko", ImmutableMessage.ValueType.STRING);
		Navajo n = NavajoFactory.getInstance().createNavajo();
		n.addMessage(StreamDocument.replicationToMessage(rm.message(),"Message",false));
		n.write(System.err);
		String mn = (String) n.getMessage("Message").getProperty("Monkey").getTypedValue();
		Assert.assertEquals("Koko", mn);
	}
}
