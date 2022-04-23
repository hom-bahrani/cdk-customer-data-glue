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
    const glueRole = new Role(this, 'glueRole', {
      roleName: 'glueCrawlerRole',
      assumedBy: new ServicePrincipal('glue.amazonaws.com'),
    });
    
    glueRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        "s3:ListBucket",
        "s3:*Object",
        "glue:CreateTable",
        "glue:GetTable",
        "glue:*"
      ],
      resources: [
        '*'
      ]
    }));

    glueRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"));
    
    const database = new glue.CfnDatabase(this, 'MyCfnDatabase', {
      // catalogId: `${process.env.ACCOUNT}`,
      catalogId: `568819880158`,
      databaseInput: {
        description: 'A test customer database',
        locationUri: 's3://appflow-test-ash/glue/data/customers_database/',
        name: databaseName,
      },
    });

    const scheduleProperty: glue.CfnCrawler.ScheduleProperty = {
      scheduleExpression: 'cron(*/5 * * * *)',
    };

    const recrawlPolicyProperty: glue.CfnCrawler.RecrawlPolicyProperty = {
      recrawlBehavior: 'CRAWL_NEW_FOLDERS_ONLY',
    };

    const crawler = new glue.CfnCrawler(this, 'MyCfnCrawler', {
      name: 'customer_test_crawler',
      description: 'A demo glue crawler',
      // schedule: scheduleProperty, TODO: Running on demand for now
      recrawlPolicy: recrawlPolicyProperty,
      role: glueRole.roleArn,
      databaseName: databaseName,
      targets: {
        s3Targets: [{
          path: 's3://appflow-test-ash/glue/data/customers_database/customers_csv/',
        }],
      },
      schemaChangePolicy: {
        deleteBehavior: 'LOG',
        updateBehavior: 'LOG',
      },
    });

    crawler.node.addDependency(database)

  }
}
