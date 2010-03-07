package base;

import java.util.HashMap;
import java.util.Map;

import com.google.apphosting.api.ApiProxy;

class TestEnvironment implements ApiProxy.Environment {
	@Override
	public String getAppId() {
		return "Unit Tests";
	}

	@Override
	public Map<String, Object> getAttributes() {
		return new HashMap<String, Object>();
	}

	@Override
	public String getAuthDomain() {
		return null;
	}

	@Override
	public String getEmail() {
		return null;
	}

	@Override
	public String getRequestNamespace() {
		return "";
	}

	@Override
	public String getVersionId() {
		return "0.1";
	}

	@Override
	public boolean isAdmin() {
		return false;
	}

	@Override
	public boolean isLoggedIn() {
		return false;
	}
}