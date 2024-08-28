package com.infybuzz.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.infybuzz.model.StudentCsv;
import com.infybuzz.model.StudentJdbc;
import com.infybuzz.model.StudentJson;
import com.infybuzz.model.StudentXml;

@Component
public class StudentItemProcessor implements ItemProcessor<StudentCsv, StudentJson>{

	@Override
	public StudentJson process(StudentCsv item) throws Exception {
		System.out.println("Inside the student processor");
		
		if(item.getId() == 6) {
			throw new NullPointerException();
		}
		
		StudentJson studentJson = new StudentJson();
		studentJson.setId(item.getId());
		studentJson.setFirstName(item.getFirstName());
		studentJson.setLastName(item.getLastName());
		studentJson.setEmail(item.getEmail());
		return studentJson;
	}
	
//	@Override
//	public StudentXml process(StudentJdbc item) throws Exception {
//		System.out.println("Inside the student processor");
//		StudentXml studentXml = new StudentXml();
//		studentXml.setId(item.getId());
//		studentXml.setFirstName(item.getFirstName());
//		studentXml.setLastName(item.getLastName());
//		studentXml.setEmail(item.getEmail());
//		return studentXml;
//	}

}
