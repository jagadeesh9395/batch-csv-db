package com.jagjava.batchcsvdb.config;

import com.jagjava.batchcsvdb.entity.Customer;
import com.jagjava.batchcsvdb.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;
import org.springframework.data.domain.Sort;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@AllArgsConstructor
public class SpringBatchConfig {

    private CustomerRepository customerRepository;
    private JpaTransactionManager jpaTransactionManager;

    @Bean
    public FlatFileItemReader<Customer> customerItemReader() {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new PathResource("E:\\work_space\\batch-csv-db\\batch-csv-db\\src\\main\\resources\\customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(customerLineMapper());
        return itemReader;

    }

    private LineMapper<Customer> customerLineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public CustomerProcessor process() {
        return new CustomerProcessor();
    }

    @Bean
    public RepositoryItemWriter<Customer> customerItemWriter() {
        RepositoryItemWriter<Customer> customerRepositoryItemWriter = new RepositoryItemWriter<>();
        customerRepositoryItemWriter.setRepository(customerRepository);
        customerRepositoryItemWriter.setMethodName("save");
        return customerRepositoryItemWriter;
    }


    //Read Data From DatBAse and store into CSV File

    @Bean
    RepositoryItemReader<Customer> readFromDataBase() {
        RepositoryItemReader<Customer> repositoryItemReader = new RepositoryItemReader<>();
        repositoryItemReader.setRepository(customerRepository);
        repositoryItemReader.setMethodName("findAll");
        Map<String, Sort.Direction> map = new HashMap<>();
        map.put("id", Sort.Direction.ASC);
        repositoryItemReader.setSort(map);
        return repositoryItemReader;

    }

    @Bean
    FlatFileItemWriter<Customer> writeIntoCSV() {
        FlatFileItemWriter<Customer> flatFileItemWriter = new FlatFileItemWriter<>();
        flatFileItemWriter.setResource(new FileSystemResource("batch-csv-db/output/customers.csv"));

        DelimitedLineAggregator<Customer> delimitedLineAggregator = new DelimitedLineAggregator<>();
        delimitedLineAggregator.setDelimiter(",");
        BeanWrapperFieldExtractor<Customer> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob"});
        delimitedLineAggregator.setFieldExtractor(fieldExtractor);
        flatFileItemWriter.setLineAggregator(delimitedLineAggregator);
        return flatFileItemWriter;
    }

    @Bean
    public Step step1(JobRepository jobRepository) {
        return new StepBuilder("import-csv-to-db-step", jobRepository)
                .<Customer, Customer>chunk(10, jpaTransactionManager)
                .reader(customerItemReader())
                .processor(process())
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    Step step2(JobRepository jobRepository) {
        return new StepBuilder("export-db-to-csv-step", jobRepository)
                .<Customer, Customer>chunk(10, jpaTransactionManager)
                .reader(readFromDataBase())
                .writer(writeIntoCSV())
                .build();
    }

    @Bean
    public Job runImportJob(JobRepository jobRepository) {
        return new JobBuilder("csv<->db-job", jobRepository)
                .start(step1(jobRepository))
                .next(step2(jobRepository))
                .build();
    }

}
