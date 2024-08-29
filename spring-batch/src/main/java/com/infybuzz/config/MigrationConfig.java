package com.infybuzz.config;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.orm.jpa.JpaTransactionManager;

import com.infybuzz.listener.SkipListenerImpl;
import com.infybuzz.postgressql.entity.Student;
import com.infybuzz.processor.StudentProcesser;

@Configuration
public class MigrationConfig {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dataSource")
	private DataSource dataSource;

	@Autowired
	@Qualifier("userDataSource")
	private DataSource userDataSource;

	@Autowired
	@Qualifier("postgresdatasource")
	private DataSource postgresdatasource;

	@Autowired
	@Qualifier("postgressqlEntityManagerFactory")
	private EntityManagerFactory postgressqlEntityManagerFactory;

	@Autowired
	@Qualifier("mysqlEntityManagerFactory")
	private EntityManagerFactory mysqlEntityManagerFactory;

	@Autowired
	private StudentProcesser studentProcesser;
	
	@Autowired
	private SkipListenerImpl skiopImpl;
	
	@Autowired
	private JpaTransactionManager jpaTransactionManager;

	@Bean
	public Job migrationData() {
		return jobBuilderFactory.get("Migrate Data")
				.incrementer(new RunIdIncrementer())
				.start(migrationSteps())
				.build();
	}

	private Step migrationSteps() {
		return stepBuilderFactory.get("Migration Step")
				.<Student, com.infybuzz.mysql.entity.Student> chunk(3)
				.reader(jpaCursorItemReader(null,null))
				.processor(studentProcesser)
				.writer(jpaItemWriter())
				.faultTolerant()
				.skip(Throwable.class)
				.skipLimit(100)
				.retryLimit(3)
				.retry(Throwable.class)
				 .listener(skiopImpl)
				 .transactionManager(jpaTransactionManager)
				.build();
	}

	@StepScope
	@Bean
	public JpaCursorItemReader<Student> jpaCursorItemReader(
			@Value("#{jobParameters['currentItemCount']}") Integer currentItemCount,
			@Value("#{jobParameters['maxItemCount']}") Integer maxItemCount) {
		JpaCursorItemReader<Student> jpaCursorItemReader = new JpaCursorItemReader<>();

		jpaCursorItemReader.setEntityManagerFactory(postgressqlEntityManagerFactory);
		jpaCursorItemReader.setQueryString("From Student");
         jpaCursorItemReader.setCurrentItemCount(currentItemCount);
         jpaCursorItemReader.setMaxItemCount(maxItemCount);
		 
		return jpaCursorItemReader;

	}

	public JpaItemWriter<com.infybuzz.mysql.entity.Student> jpaItemWriter() {
		JpaItemWriter<com.infybuzz.mysql.entity.Student> jpaItemWriter = new JpaItemWriter<>();

		jpaItemWriter.setEntityManagerFactory(mysqlEntityManagerFactory);
		return jpaItemWriter;
	}

}
