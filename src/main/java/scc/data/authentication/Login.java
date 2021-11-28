package scc.data.authentication;

public class Login {
	private String idUser;
	private String pwd;

	public Login() {
	}

	public Login(String idUser, String pwd) {
		this.idUser = idUser;
		this.pwd = pwd;
	}

	public String getIdUser() {
		return idUser;
	}

	public void setIdUser(String idUser) {
		this.idUser = idUser;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
}
