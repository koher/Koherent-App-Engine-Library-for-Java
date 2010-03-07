package base;

import java.io.File;

import com.google.appengine.tools.development.ApiProxyLocalImpl;
import com.google.apphosting.api.ApiProxy;

import junit.framework.TestCase;

public class LocalServiceTestCase extends TestCase {
	@Override
	public void setUp() throws Exception {
		super.setUp();
		ApiProxy.setEnvironmentForCurrentThread(new TestEnvironment());
		ApiProxy.setDelegate(new ApiProxyLocalImpl(new File(".")) {
		});
	}

	@Override
	public void tearDown() throws Exception {
		ApiProxy.setDelegate(null);
		ApiProxy.setEnvironmentForCurrentThread(null);
		super.tearDown();
	}
}
