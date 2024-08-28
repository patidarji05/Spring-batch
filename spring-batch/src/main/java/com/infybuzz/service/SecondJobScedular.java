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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class SecondJobScedular {

	@Autowired
	JobLauncher jobLauncher;

	@Qualifier("seconJob")
	@Autowired
	Job seconJob;

	//@Scheduled(cron = "0 0/1 * 1/1 * ?")
	public void secondJobStarter() {
		Map<String, JobParameter> parms = new HashMap<>();
		parms.put("currentime", new JobParameter(System.currentTimeMillis()));

		JobParameters jobParameters = new JobParameters(parms);
		try {
			JobExecution jobExecution = jobLauncher.run(seconJob, jobParameters);
			System.out.println("Job Execution ID :" + jobExecution.getId());
		} catch (Exception e) {
			System.out.println("job failed...execption occure");
		}

	}

}
