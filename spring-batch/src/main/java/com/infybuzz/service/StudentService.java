package com.infybuzz.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.infybuzz.model.StudentResponse;

@Service
public class StudentService {

	List<StudentResponse> list;

//	public List<StudentResponse> restCallToGetStudents() {
//		RestTemplate restTemplate = new RestTemplate();
//		StudentResponse[] studentResponseArray = restTemplate.getForObject("https://dummyjson.com/users?select=id,email",
//				StudentResponse[].class);
//
//		list = new ArrayList<>();
//
//		for (StudentResponse sr : studentResponseArray) {
//			list.add(sr);
//		}
//
//		return list;
//	}
//
//	public StudentResponse getStudent(long id, String email) {
//		System.out.println("id = " + id + " and name = " + email);
//		if (list == null) {
//			restCallToGetStudents();
//		}
//
//		if (list != null && !list.isEmpty()) {
//			return list.remove(0);
//		}
//		return null;
//	}
}
