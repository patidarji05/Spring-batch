package com.infybuzz.model;

public class StudentResponse {

	private Long id;


	private String email;

	public StudentResponse() {

	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	@Override
	public String toString() {
		return "StudentResponse [id=" + id + ", email=" + email + "]";
	}

	

}
