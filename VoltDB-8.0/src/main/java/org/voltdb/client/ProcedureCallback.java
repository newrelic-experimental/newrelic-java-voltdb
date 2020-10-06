package org.voltdb.client;

import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Segment;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.MatchType;
import com.newrelic.api.agent.weaver.NewField;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;

@Weave(type=MatchType.Interface)
public abstract class ProcedureCallback {
	
	@NewField
	public Segment segment = null;

	@NewField
	public DatastoreParameters params = null;
	
	@Trace(async=true)
	public void clientCallback(ClientResponse clientResponse) {
		if(segment != null) {
			if(params != null) {
				segment.reportAsExternal(params);
			}
			segment.end();
			segment = null;
		} else if(params != null) {
			NewRelic.getAgent().getTracedMethod().reportAsExternal(params);
		}
		Weaver.callOriginal();
	}
}
