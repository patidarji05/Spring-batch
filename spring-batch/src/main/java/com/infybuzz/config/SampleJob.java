package com.infybuzz.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.infybuzz.listener.FirstJobListener;
import com.infybuzz.listener.FirstStepListener;
import com.infybuzz.processor.FirstItemProcesser;
import com.infybuzz.reader.FirestItemReader;
import com.infybuzz.service.SecondTask;
import com.infybuzz.writer.FirstItemWriter;

@Configuration
public class SampleJob {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private SecondTask secondTask;

	@Autowired
	private FirstStepListener firstStepListener;

	@Autowired
	private FirstJobListener firstJobListner;

	@Autowired
	private FirestItemReader firsFirestItemReader;

	@Autowired
	private FirstItemProcesser firstItemProcesser;

	@Autowired
	private FirstItemWriter firstItemWriter;

	@Bean
	public Job seconJob() {
		return jobBuilderFactory.get("second job")
				.incrementer(new RunIdIncrementer())
				.start(firstChunkStep())
				.next(secondStep())
				.build();

	}

	private Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step")
				.<Integer, Long>chunk(3)
				.reader(firsFirestItemReader)
				.processor(firstItemProcesser)
				.writer(firstItemWriter).build();
	}

	 @Bean
	public Job firstJob() {
		return jobBuilderFactory.get("First Job").incrementer(new RunIdIncrementer()).start(firstStep())
				.next(secondStep()).listener(firstJobListner).build();
	}

	private Step firstStep() {
		return stepBuilderFactory.get("First Step").tasklet(firstTask()).listener(firstStepListener).build();
	}

	private Tasklet firstTask() {
		return new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("This is first tasklet step");
				System.out.println(chunkContext.getStepContext().getStepExecutionContext());
				return RepeatStatus.FINISHED;
			}
		};
	}

	private Step secondStep() {
		return stepBuilderFactory.get("second Step").tasklet(secondTask).build();
	}

}
