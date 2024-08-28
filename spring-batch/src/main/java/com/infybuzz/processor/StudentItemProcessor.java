package com.infybuzz.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.infybuzz.model.StudentJdbc;
import com.infybuzz.model.StudentJson;

@Component
public class StudentItemProcessor implements ItemProcessor<StudentJdbc, StudentJson>{

	@Override
	public StudentJson process(StudentJdbc item) throws Exception {
		System.out.println("Inside the student processor");
		StudentJson studentJson = new StudentJson();
		studentJson.setId(item.getId());
		studentJson.setFirstName(item.getFirstName());
		studentJson.setLastName(item.getLastName());
		studentJson.setEmail(item.getEmail());
		return studentJson;
	}

}
