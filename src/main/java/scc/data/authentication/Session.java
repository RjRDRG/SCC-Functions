package scc.data.authentication;

import java.util.UUID;

public class Session {

	private String idUser;
	private String sessionId;

	public Session() {
		
	}

	public Session(String idUser) {
		this.sessionId = "session:"+ UUID.randomUUID();
		this.idUser = idUser;
	}

	public String getIdUser() {
		return idUser;
	}

	public void setIdUser(String idUser) {
		this.idUser = idUser;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	public String toString() {
		return "Session{" +
				"idUser='" + idUser + '\'' +
				", sessionId='" + sessionId + '\'' +
				'}';
	}
}
