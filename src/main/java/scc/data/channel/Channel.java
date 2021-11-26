package scc.data.channel;

import java.util.Arrays;

public class Channel {
	private String idChannel;
	private String name;
	private String owner;
	private boolean publicChannel;
	private String[] members;

	public Channel(String idChannel, String name, String owner, boolean publicChannel, String[] members) {
		super();
		this.idChannel = idChannel;
		this.members = members;
		this.name = name;
		this.owner = owner;
		this.publicChannel = publicChannel;
	}

	public String getIdChannel() {
		return idChannel;
	}

	public void setIdChannel(String idChannel) {
		this.idChannel = idChannel;
	}

	public String[] getMembers() {
		return members;
	}

	public void setMembers(String[] members) {
		this.members = members;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public void setVisibility(boolean publicChannel) {
		this.publicChannel = publicChannel;
	}
	
	public boolean getVisibility() {
		return publicChannel;
	}

	@Override
	public String toString() {
		return "Channel[idChannel=" + idChannel + ", name=" + name + ", owner=" + owner + ", publicChannel= " + publicChannel
				+ ", usersIds=" + Arrays.toString(members) + "]";
	}

}
