import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { aws_glue as glue } from 'aws-cdk-lib';
import {
  ManagedPolicy,
  ServicePrincipal,
  Role,
  PolicyStatement,
  Effect
} from 'aws-cdk-lib/aws-iam';

export class TddSparkAwsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const databaseName = 'customer_database';

    const database = new glue.CfnDatabase(this, 'MyCfnDatabase', {
      // catalogId: `${process.env.ACCOUNT}`,
      catalogId: `568819880158`,
      databaseInput: {
        description: 'A test customer database',
        locationUri: 's3://appflow-test-ash/glue/data/customers_database/',
        name: databaseName,
      },
    });

    const table = new glue.CfnTable(this, 'MyCfnTable', {
      // catalogId: `${process.env.ACCOUNT}`,
      catalogId: '568819880158',
      databaseName: databaseName,
      tableInput: {
        description: 'A test table',
        name: 'test_customer_csv',
        retention: 123,
        storageDescriptor: {
          inputFormat: 'csv',
          location: 's3://appflow-test-ash/glue/data/customers_database/customers_csv/',
        },
      },
    });

    const glueRole = new Role(this, 'glueRole', {
      roleName: 'glueCrawlerRole',
      assumedBy: new ServicePrincipal('glue.amazonaws.com'),
    });
    
    glueRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "s3:ListBucket",
        "s3:*Object",
        "glue:CreateTable"
      ],
      resources: [
        '*'
      ]
    }));

    const crawler = new glue.CfnCrawler(this, 'MyCfnCrawler', {
      role: glueRole.roleArn,
      targets: {
        catalogTargets: [{
          databaseName: 'databaseName',
          tables: [ 'test_customer_csv' ],
        }]
      },
      schemaChangePolicy: {
        deleteBehavior: 'LOG',
        updateBehavior: 'LOG',
      },
    });

  }
}
