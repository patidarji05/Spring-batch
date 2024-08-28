package com.infybuzz.config;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.infybuzz.listener.SkipListener;
import com.infybuzz.listener.SkipListenerImpl;
import com.infybuzz.model.StudentCsv;
import com.infybuzz.model.StudentJson;
import com.infybuzz.processor.StudentItemProcessor;

@Configuration
public class SampleHandle {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private StudentItemProcessor studentItemProcessor;

//	@Autowired
//	private SkipListener skipListener;
	
	@Autowired
	private SkipListenerImpl skiopImpl; 

	@Bean
	public Job chunkJob() {

		return jobBuilderFactory.get("Chunk Job").incrementer(new RunIdIncrementer()).start(firstChunkStep()).build();

	}

	public Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step").<StudentCsv, StudentJson>chunk(3)
				.reader(flateFileItemReader1(null)).processor(studentItemProcessor).writer(jsonFileItemWriter1(null))
				.faultTolerant().skip(Throwable.class)
				// .skip(FlatFileParseException.class)
				// .skip(Exception.class)
				 .skipLimit(100)
				//.skipPolicy(new AlwaysSkipItemSkipPolicy())
				.retryLimit(3)
				.retry(Throwable.class)
			//	.listener(skipListener)
				.listener(skiopImpl)
				.build();

	}

	@StepScope
	@Bean
	public FlatFileItemReader<StudentCsv> flateFileItemReader1(
			@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
		FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<StudentCsv>();

		flatFileItemReader.setResource(fileSystemResource);

		DefaultLineMapper<StudentCsv> defaultLineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
		delimitedLineTokenizer.setNames("ID", "First Name", "Last Name", "Email");

		defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);

		BeanWrapperFieldSetMapper<StudentCsv> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(StudentCsv.class);

		defaultLineMapper.setFieldSetMapper(fieldSetMapper);

		flatFileItemReader.setLineMapper(defaultLineMapper);

		flatFileItemReader.setLinesToSkip(1);
		return flatFileItemReader;
	}

	@StepScope
	@Bean
	public JsonFileItemWriter<StudentJson> jsonFileItemWriter1(
			@Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {

		JsonFileItemWriter<StudentJson> jsonFileItemWriter = new JsonFileItemWriter<StudentJson>(fileSystemResource,
				new JacksonJsonObjectMarshaller<StudentJson>()) {

			@Override
			public String doWrite(List<? extends StudentJson> item) {
				item.stream().forEach(items -> {
					if(items.getId() == 5) {
						System.out.println("inside the json item wrietr");
						throw new NullPointerException();
					}
				});
				return super.doWrite(item);
			}
		};

		return jsonFileItemWriter;

	}
}
