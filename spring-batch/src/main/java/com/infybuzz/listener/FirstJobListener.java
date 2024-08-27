package com.infybuzz.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class FirstJobListener implements JobExecutionListener {

	@Override
	public void beforeJob(JobExecution jobExecution) {
		System.out.println("this is befor job execution" + jobExecution.getJobInstance().getJobName());
		System.out.println("Job Params: " + jobExecution.getJobParameters());
		System.out.println("Job executionContext: " + jobExecution.getExecutionContext());
		jobExecution.getExecutionContext().put("abc", "abc value");
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		System.out.println("this is After job" + jobExecution.getJobInstance().getJobName());
		System.out.println("Job Params: " + jobExecution.getJobParameters());
		System.out.println("Job executionContext: " + jobExecution.getExecutionContext());
	}

}
