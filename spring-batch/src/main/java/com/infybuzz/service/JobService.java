package com.infybuzz.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class JobService {

	@Autowired
	JobLauncher jobLauncher;

	@Qualifier("firstJob")
	@Autowired
	Job firstJob;

	@Qualifier("seconJob")
	@Autowired
	Job seconJob;

	@Async
	public void startJob(String jobName) {

		Map<String, JobParameter> parms = new HashMap<>();
		parms.put("currentime", new JobParameter(System.currentTimeMillis()));

		JobParameters jobParameters = new JobParameters(parms);
		try {
			JobExecution jobExecution = null;
			if (jobName.equals("First Job")) {
				jobExecution = jobLauncher.run(firstJob, jobParameters);
			} else if (jobName.equals("second job")) {
				jobExecution = jobLauncher.run(seconJob, jobParameters);
			}
			System.out.println("Job Execution ID :" + jobExecution.getId());
		} catch (Exception e) {
			System.out.println("job failed...execption occure");
		}

	}

}
