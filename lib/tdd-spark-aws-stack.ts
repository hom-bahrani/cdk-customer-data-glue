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
import { Asset } from 'aws-cdk-lib/aws-s3-assets';
import * as path from 'path';

export class TddSparkAwsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const databaseName = 'customer_database';
    const tableName = 'customers_csv';

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

    const scriptAsset = new Asset(this, 'Script', {
      path: path.join(__dirname, '..', 'scripts', 'job1.py'),
    });

    scriptAsset.grantRead(glueRole);

    const job = new glue.CfnJob(this, 'Job', {
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${scriptAsset.s3BucketName}/${scriptAsset.s3ObjectKey}`,
      },
      numberOfWorkers: 2,
      role: glueRole.roleArn,
      glueVersion: '2.0',
      name: 'AwsGlueEtlSampleCdk',
      defaultArguments: {
        '--job-bookmark-option': 'job-bookmark-enable',
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
        '--DATABASE_NAME': databaseName,
        '--TABLE_NAME': tableName,
        '--OUTPUT_BUCKET': 'appflow-test-ash',
        '--OUTPUT_PATH': '/glue/data/customers_database/customers_json/',
      },
      timeout: 60 * 24,
      workerType: 'G.2X'
    });

  }
}
