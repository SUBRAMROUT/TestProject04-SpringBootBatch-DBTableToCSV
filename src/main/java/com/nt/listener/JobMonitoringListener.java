package com.nt.listener;

import java.util.Date;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;


@Component("jmListener")
public class JobMonitoringListener implements JobExecutionListener {
	private long start,end;
	
	@Override
	public void beforeJob(JobExecution jobExecution) {
		start=System.currentTimeMillis();
		System.out.println("Job started at :: "+new Date());
	}
	
	@Override
	public void afterJob(JobExecution jobExecution) {
		end=System.currentTimeMillis();
		System.out.println("Job Ended at ::"+new Date());
		System.out.println("Job Execution Status ::"+jobExecution.getExitStatus());
	}

}
