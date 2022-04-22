import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { aws_glue as glue } from 'aws-cdk-lib';

export class TddSparkAwsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    
    const database = new glue.CfnDatabase(this, 'MyCfnDatabase', {
      catalogId: `${process.env.ACCOUNT}`,
      databaseInput: {
        description: 'A test customer database',
        locationUri: 's3://appflow-test-ash/glue/data/customers_database/',
        name: 'customer_database',
      },
    });

  }
}
