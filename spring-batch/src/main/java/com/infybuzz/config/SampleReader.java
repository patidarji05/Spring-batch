package com.infybuzz.config;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.infybuzz.model.StudentCsv;
import com.infybuzz.model.StudentJdbc;
import com.infybuzz.model.StudentJson;
import com.infybuzz.model.StudentXml;

/*
 * This  config file is used to read data from CSV , Json, xml, db file 
 */
@Configuration
public class SampleReader {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	 @Autowired
	 private DataSource dataSource;

	

	@Bean
	public Job chunkJob() {

		return jobBuilderFactory.get("Chunk Job").incrementer(new RunIdIncrementer()).start(firstChunkStep()).build();

	}

	public Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step").<StudentJdbc, StudentJdbc>chunk(3)
				// .reader(flateFileItemReader(null))
				// .reader(jsonItemReader(null))
				// .reader(staxEventItemReader(null))
				.reader(jdbcCursorItemReader())
				// .processor(firstItemProcesser)
				.writer(itemWriter()).build();
	}

	// read jaba from db

	public JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader() {
		JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader = new JdbcCursorItemReader<>();

		jdbcCursorItemReader.setDataSource(dataSource);
		jdbcCursorItemReader.setSql("select id, first_name as firstName, last_name as lastName, email from student");

		jdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<StudentJdbc>() {

			{
				setMappedClass(StudentJdbc.class);
			}

		});

		jdbcCursorItemReader.setCurrentItemCount(2);
		jdbcCursorItemReader.setMaxItemCount(8);
		return jdbcCursorItemReader;
	}

	// read the data form xml file

	@StepScope
	// @Bean
	public StaxEventItemReader<StudentXml> staxEventItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {

		StaxEventItemReader<StudentXml> staxEventItemReader = new StaxEventItemReader<>();
		staxEventItemReader.setResource(fileSystemResource);
		staxEventItemReader.setFragmentRootElementName("student");
		staxEventItemReader.setUnmarshaller(new Jaxb2Marshaller() {
			{
				setClassesToBeBound(StudentXml.class);
			}
		});

		return staxEventItemReader;
	}

	// read tha data from json file
	@StepScope
	@Bean
	public JsonItemReader<StudentJson> jsonItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {

		JsonItemReader<StudentJson> jsonItemReader = new JsonItemReader<>();
		jsonItemReader.setResource(fileSystemResource);

		jsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJson.class));

		jsonItemReader.setMaxItemCount(8);
		jsonItemReader.setCurrentItemCount(2);

		return jsonItemReader;

	}

	// read value from jobparameter

	// public FlatFileItemReader<StudentCsv> flateFileItemReader() {

	@StepScope
	@Bean
	public FlatFileItemReader<StudentCsv> flateFileItemReader(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
		FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<StudentCsv>();

//		flatFileItemReader.setResource(new FileSystemResource(
//				new File("C:\\Users\\prade\\git\\spring-batch\\spring-batch\\InputFiles\\student.csv")));

		flatFileItemReader.setResource(fileSystemResource);

//		flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCsv>() {
//			{
//				setLineTokenizer(new DelimitedLineTokenizer("|") {
//					{
//						setNames("ID", "First Name", "Last Name", "Email");
//						// setDelimiter("|");
//					}
//				});
//
//				setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCsv>() {
//					{
//						setTargetType(StudentCsv.class);
//					}
//				});
//			}
//		});

		DefaultLineMapper<StudentCsv> defaultLineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer("|");
		delimitedLineTokenizer.setNames("ID", "First Name", "Last Name", "Email");

		defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);

		BeanWrapperFieldSetMapper<StudentCsv> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(StudentCsv.class);

		defaultLineMapper.setFieldSetMapper(fieldSetMapper);

		flatFileItemReader.setLineMapper(defaultLineMapper);

		flatFileItemReader.setLinesToSkip(1);
		return flatFileItemReader;
	}

	public ItemWriter<StudentJdbc> itemWriter() {

		return new ItemWriter<StudentJdbc>() {

			@Override
			public void write(List<? extends StudentJdbc> items) throws Exception {
				System.out.println("Inside the item reader");
				items.stream().forEach(System.out::println);

			}
		};

	}

}
