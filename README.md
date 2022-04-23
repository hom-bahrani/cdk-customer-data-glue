# Simple ETL jobs with spark and AWS Glue

In this repo you will find CDK exmaple of how you can build and run serverless spark jobs on AWS Glue. 

Glue is an excellent choice for processing Terabyte (TB) of data, and can integrate into S3, Serverless Aurora, Redshift for data warehousing and Sagemaker for Machine Learning. 

The number of AWS Glue data processing units (DPUs) that can be allocated when a job runs can be set by the `maxCapacity` property in the CDK code. A DPU is a relative measure of processing power that consists of 4 vCPUs of compute capacity and 16 GB of memory (you can allocate from 2 to 100 DPUs. The default is 10 DPUs)

<img width="682" alt="Screenshot 2022-04-23 at 12 34 58" src="https://user-images.githubusercontent.com/8465628/164892783-13ac6ca0-df3d-4161-8332-ed448a8424f0.png">
