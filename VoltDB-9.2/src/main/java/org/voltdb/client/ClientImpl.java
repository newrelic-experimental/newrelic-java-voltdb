package org.voltdb.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;

@Weave
public abstract class ClientImpl {

	@Trace
	public boolean callAllPartitionProcedure(AllPartitionProcedureCallback callback, String procedureName,
            Object... params) throws IOException, ProcCallException { 		
		boolean result = false;
		DatastoreParameters dsParams = DatastoreParameters.product("VoltDB").collection(procedureName).operation("callAllPartitionProcedure").noInstance().noDatabaseName().build();
		if(callback != null) {
			if(callback.segment == null) {
				String opName = "callAllPartitionProcedure";
				callback.segment = NewRelic.getAgent().getTransaction().startSegment("VoltDB-"+opName+"-"+procedureName);
			}
			callback.params = dsParams;
		}
		try {
			result = Weaver.callOriginal();
		} catch (Exception e) {
			if(IOException.class.isInstance(e)) {
				NewRelic.noticeError(e);
				throw (IOException)e;
			} else if(ProcCallException.class.isInstance(e)) {
				NewRelic.noticeError(e);
				throw (ProcCallException)e;
			}
		}
		return result;
	}
	
	@Trace
	protected ClientResponse callProcedureWithClientTimeoutImpl(
            int batchTimeout,
            String procName,
            long clientTimeout,
            TimeUnit unit,
            Object... parameters) throws IOException, NoConnectionsException, ProcCallException
    {
		DatastoreParameters dsParams = DatastoreParameters.product("VoltDB").collection(procName).operation("callProcedureWithClientTimeout").noInstance().noDatabaseName().build();
		NewRelic.getAgent().getTracedMethod().reportAsExternal(dsParams);
		ClientResponse result = null;
		try {
			result = Weaver.callOriginal();
		} catch (Exception e) {
			if(IOException.class.isInstance(e)) {
				NewRelic.noticeError(e);
				throw (IOException)e;
			} else if(NoConnectionsException.class.isInstance(e)) {
				NewRelic.noticeError(e);
				throw (NoConnectionsException)e;
			} else if(ProcCallException.class.isInstance(e)) {
				NewRelic.noticeError(e);
				throw (ProcCallException)e;
			}
		}
		return result;
    }
	
	@Trace
	public boolean callProcedureWithClientTimeout(
            ProcedureCallback callback,
            int batchTimeout,
            int partitionDestination,
            String procName,
            long clientTimeout,
            TimeUnit clientTimeoutUnit,
            Object... parameters) throws IOException
    {
		DatastoreParameters dsParams = DatastoreParameters.product("VoltDB").collection(procName).operation("callProcedureWithClientTimeout").noInstance().noDatabaseName().build();
		if(callback != null) {
			if(callback.segment == null) {
				String opName = "callAllPartitionProcedure";
				callback.segment = NewRelic.getAgent().getTransaction().startSegment("VoltDB-"+opName+"-"+procName);
			}
			callback.params = dsParams;
		}
		boolean result = false;
		try {
			result = Weaver.callOriginal();
		} catch (Exception e) {
			if(IOException.class.isInstance(e)) {
				NewRelic.noticeError(e);
				throw (IOException)e;
			}
		}
		return result;
    }
	
	
}
