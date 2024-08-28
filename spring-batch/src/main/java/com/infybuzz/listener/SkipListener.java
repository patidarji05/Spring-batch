package com.infybuzz.listener;

import java.io.File;
import java.io.FileWriter;

import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import com.infybuzz.model.StudentCsv;
import com.infybuzz.model.StudentJson;

@Component
public class SkipListener {
	
	@OnSkipInRead
	public void onSkipInRead(Throwable th) {
		
		if(th instanceof FlatFileParseException) {
			createFile("C:\\Users\\prade\\git\\spring-batch\\spring-batch\\Chunk Job\\First Chunk Step\\reader\\SkipInRead.txt",
				((FlatFileParseException) th).getInput());
		}
		
	}
	
	@OnSkipInProcess
	public void onSkipProcess(StudentCsv studentCsv,Throwable th) {
			createFile("C:\\Users\\prade\\git\\spring-batch\\spring-batch\\Chunk Job\\First Chunk Step\\processer\\SkipInProcess.txt",
					studentCsv.toString());
	}
	
	@OnSkipInWrite
	public void onSkipInWrite(StudentJson studentJson, Throwable th) {
		createFile("C:\\Users\\prade\\git\\spring-batch\\spring-batch\\Chunk Job\\First Chunk Step\\writer\\SkipInWrite.txt",
				studentJson.toString());
		
	}

	public void createFile(String filePath, String data) {
		try(FileWriter fileWriter = new FileWriter(new File(filePath), true)) {
			fileWriter.write(data + "\n");
			
		}catch (Exception e) {
		}
	}
}
