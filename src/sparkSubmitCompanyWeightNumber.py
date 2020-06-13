import sys
import time
import gzip
import boto3
import json

# FUNCTIONS:
# Returns a dictionary
def lambda_handler(event, context):
    s3r = boto3.resource('s3')
    bucket = 'ssd-test-dev'
    filename = 'sparkCompanyWeightNumber.py'
    # S3 directory
    directoryS3 = ''
    # local directory
    directoryLc = ''
    # Where the file will go on s3
    # k = directory and filename
    k = directoryS3 + filename
    # Upload Location on S3 Bucket
    locFile = directoryLc + filename

    didItWork = s3r.Bucket(bucket).upload_file(locFile,k)
    print(didItWork)

    conn = boto3.client("emr")
    # chooses the first cluster which is Running or Waiting
    # possibly can also choose by name or already have the cluster id
    clusters = conn.list_clusters()
    # choose the correct cluster
    clusters = [c["Id"] for c in clusters["Clusters"]
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]
    if not clusters:
        sys.stderr.write("No valid clusters\n")
        sys.stderr.exit()
    # take the first relevant cluster
    cluster_id = clusters[0]
    print(cluster_id)

    # code location on your emr master node (hard coded)
    #CODE_DIR = "/home/hadoop/"
    CODE_DIR = 's3://'+ bucket + '/'

    # spark configuration example
    # took out these steps:
    # CODE_DIR + 'wordcount.py',
    # By CLI: $ spark-submit --master yarn --deploy-mode cluster /home/hadoop/wordcount.py s3a://covid19-lake/static-datasets/csv/state-abv/states_abv.csv
    # step_args is a translation of the CLI into list format
    # The important thing is to have the python file be written in PySpark and be accessible by the cluster.
    step_args2 = [
        '/usr/bin/spark-submit',
        '--master', 'yarn',
        '--deploy-mode', 'client',
        CODE_DIR + filename,
        's3://ssd-package-s3-dev/ams/2020/202005251500/ams__header_2020__202005251500.csv',
        's3://ssd-package-s3-dev/ams/2020/202005251500/ams__consignee_2020__202005251500.csv'
    ]

    step_args1 = [
        '/usr/bin/spark-submit',
        '--master', 'yarn',
        '--deploy-mode', 'client',
        CODE_DIR + filename,
        's3://ssd-package-s3-dev/ams/2018/202001290000/ams__header_2018__202001290000.csv',
        's3://ssd-package-s3-dev/ams/2018/202001290000/ams__consignee_2018__202001290000.csv'
    ]

    step_args = [
        '/usr/bin/spark-submit',
        '--master', 'yarn',
        '--deploy-mode', 'client',
        CODE_DIR + filename,
        's3://ssd-package-s3-dev/ams/2019/ams__header_2019__202001080000.csv',
        's3://ssd-package-s3-dev/ams/2019/ams__consignee_2019__202001080000.csv'
    ]

    # ActionOnFailure options: 'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE',
    step = {"Name": "what_you_do-" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': step_args
            }
            }
    step1 = {"Name": "what_you_do-" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': step_args1
            }
            }
    step2 = {"Name": "what_you_do-" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': step_args2
            }
            }
    action = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    action1 = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step1])
    action2 = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step2])
#    return "Added step: %s" % (action)

    # Returns a dictionary
    return action

# Execute Lambda
response = lambda_handler(0,0)

folderLogLoc = response['StepIds'][0]

print(folderLogLoc)

