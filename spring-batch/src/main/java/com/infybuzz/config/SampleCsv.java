package com.infybuzz.config;

import java.io.File;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.infybuzz.model.StudentCsv;

/*
 * This  config file is used to read data from CSV file 
 */
@Configuration
public class SampleCsv {

	@Autowired
	JobBuilderFactory jobBuilderFactory;

	@Autowired
	StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job chunkJob() {

		return jobBuilderFactory.get("Chunk Job").incrementer(new RunIdIncrementer()).start(firstChunkStep()).build();

	}

	public Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step").<StudentCsv, StudentCsv>chunk(3)
				.reader(flateFileItemReader(null))
				// .processor(firstItemProcesser)
				.writer(itemWriter()).build();
	}

	// public FlatFileItemReader<StudentCsv> flateFileItemReader() {
	
	// read value from jobparameter
	@StepScope
	@Bean
	public FlatFileItemReader<StudentCsv> flateFileItemReader(@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
		FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<StudentCsv>();

//		flatFileItemReader.setResource(new FileSystemResource(
//				new File("C:\\Users\\prade\\git\\spring-batch\\spring-batch\\InputFiles\\student.csv")));

		flatFileItemReader.setResource(fileSystemResource);
		
		flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCsv>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer("|") {
					{
						setNames("ID", "First Name", "Last Name", "Email");
					//	setDelimiter("|");
					}
				});

				setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCsv>() {
					{
						setTargetType(StudentCsv.class);
					}
				});
			}
		});
		flatFileItemReader.setLinesToSkip(1);
		return flatFileItemReader;
	}

	public ItemWriter<StudentCsv> itemWriter() {

		return new ItemWriter<StudentCsv>() {

			@Override
			public void write(List<? extends StudentCsv> items) throws Exception {
				System.out.println("Inside the item reader");
				items.stream().forEach(System.out::println);

			}
		};

	}

}
