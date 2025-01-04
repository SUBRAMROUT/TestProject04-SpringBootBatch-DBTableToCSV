package com.nt.config;


import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.nt.listener.JobMonitoringListener;
import com.nt.model.ExamResult;
import com.nt.processor.ExamResultItemProcessor;

@Configuration
public class BatchConfig {
	
	@Autowired
	private JobMonitoringListener listener;
	@Autowired
	private ExamResultItemProcessor processor;
	@Autowired
	private DataSource ds;
	
	/*@Bean
	public JdbcCursorItemReader<ExamResult>createReader(){
		//create jdbcCursorItemReader object
		JdbcCursorItemReader<ExamResult> reader = new JdbcCursorItemReader<ExamResult>();
		//set DataSource
		reader.setDataSource(ds);
		//Specify the query
		reader.setSql("SELECT * FROM EXAM_RESULT");
		//set RowMapper to convert each record into ExamResult object
		reader.setRowMapper((rs,rowNum)-> new ExamResult(rs.getInt(1),rs.getDate(2),rs.getInt(3),rs.getFloat(4)));
		return reader;
	}*/
	
	@Bean
	public JdbcCursorItemReader<ExamResult> createReader(){
		return new JdbcCursorItemReaderBuilder<ExamResult>()
				.name("Jdbc-reader")
				.dataSource(ds)
				.sql("SELECT * FROM EXAM_RESULT")
				.beanRowMapper(ExamResult.class)
				.build();
	}
	 
	/*@Bean
	public FlatFileItemWriter<ExamResult> createWriter(){
		//create writer object
		FlatFileItemWriter<ExamResult> writer = new FlatFileItemWriter<ExamResult>();
		//specify the location
		writer.setResource(new FileSystemResource("SuperBrain.csv"));
		//field Extractor
		BeanWrapperFieldExtractor<ExamResult> extractor=new BeanWrapperFieldExtractor<ExamResult>();
		extractor.setNames(new String[] {"id","dob","semester","percentage"});
		//line Aggregator
		DelimitedLineAggregator<ExamResult> aggregator=new DelimitedLineAggregator<ExamResult>();
		aggregator.setDelimiter(",");
		aggregator.setFieldExtractor(extractor);
		//set aggregator to write
		writer.setLineAggregator(aggregator);
		return writer;
	}*/
	
	@Bean
	public FlatFileItemWriter<ExamResult> createWriter(){
		return new FlatFileItemWriterBuilder<ExamResult>()
				.name("file-writer")
				.resource(new FileSystemResource("SuperBrains.csv"))
				.lineSeparator("\r\n")
				.delimited().delimiter(",")
				.names("id","dob","semester","percentage")
				.build();
	}
	
	@Bean(name="step1")
	public Step createStep1(JobRepository repository,
			                         PlatformTransactionManager txMgmr) {
		System.out.println("BatchConfig.createStep1()");
		return new StepBuilder("step1",repository)
				.<ExamResult, ExamResult>chunk(2,txMgmr)
				.reader(createReader())
				.processor(processor)
				.writer(createWriter())
				.build();
	}
	
	@Bean(name="job1")
	public Job createJob1(JobRepository repository, Step step1) {
		System.out.println("BatchConfig.createJob1()");
		return new JobBuilder("job1",repository)
		        .listener(listener)
		        .incrementer(new RunIdIncrementer())
		        .start(step1)
		        .build();
	}

}
