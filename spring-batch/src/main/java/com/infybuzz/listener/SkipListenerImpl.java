package com.infybuzz.listener;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;

import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import com.infybuzz.model.StudentCsv;
import com.infybuzz.model.StudentJson;

@Component
public class SkipListenerImpl implements SkipListener<StudentCsv, StudentJson>{

	@Override
	public void onSkipInRead(Throwable t) {
		if(t instanceof FlatFileParseException) {
			createFile("C:\\Users\\prade\\git\\spring-batch\\spring-batch\\Chunk Job\\First Chunk Step\\reader\\SkipInRead.txt",
				((FlatFileParseException) t).getInput());
		}
		
	}

	@Override
	public void onSkipInWrite(StudentJson item, Throwable t) {
		createFile("C:\\Users\\prade\\git\\spring-batch\\spring-batch\\Chunk Job\\First Chunk Step\\writer\\SkipInWrite.txt",
				item.toString());
		
	}

	@Override
	public void onSkipInProcess(StudentCsv item, Throwable t) {
		createFile("C:\\Users\\prade\\git\\spring-batch\\spring-batch\\Chunk Job\\First Chunk Step\\processer\\SkipInProcess.txt",
				item.toString());
		
	}
	
	public void createFile(String filePath, String data) {
		try(FileWriter fileWriter = new FileWriter(new File(filePath), true)) {
			fileWriter.write(data +","+ new Date()+ "\n");
			
		}catch (Exception e) {
		}
	}

}
