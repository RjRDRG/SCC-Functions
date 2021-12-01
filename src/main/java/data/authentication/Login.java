package data.authentication;

public class Login {
	private String user;
	private String pwd;

	public Login() {
	}

	public Login(String user, String pwd) {
		this.user = user;
		this.pwd = pwd;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	@Override
	public String toString() {
		return "Login{" +
				"user='" + user + '\'' +
				", pwd='" + pwd + '\'' +
				'}';
	}
}
