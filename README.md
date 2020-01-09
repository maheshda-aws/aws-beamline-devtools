
# AWSBeamlineDevtools
AWS Beamline Devtools is a utility for users to perform interactive data analysis  using jupyter notebooks on top of Spark on EMR. This command line tool makes it easy for analysts and data engineers to manage dedicated or shared sandbox spark clusters on EMR for interactive querying and development. 

Users can attach their notebook instances with existing shared/dedicated EMR cluster or they can also create a new EMR cluster by providing appropriate parameters. 

## Architecture Diagram
![Architecture](https://i.ibb.co/kKHdB6r/Beamline-Dev-Tools.jpg)

Please refer to: https://aws.amazon.com/blogs/machine-learning/build-amazon-sagemaker-notebooks-backed-by-spark-in-amazon-emr/


## Set up

 - As a prerequisite, please make sure the role associated with notebook instance has access to create new EMR clusters if you intend to create one.
 - Please make sure the security group associated with EMR master node allows traffic from the security group of the notebook instance.
 - Go to AWS console and create a sagemaker notebook lifecycle configuration by copying the content from `notebook-lifecycle-config.sh`
 - Spin up a sagemaker notebook server using the lifecycle configuration created.
 - Open the terminal on the notebook instance once the server is ready.
 - Execute `./attach-emr` cli

## CLI usage

    ./attach-emr -h
    
    Usage: attach-emr [options] filename
    
    Options:
      --version             show program's version number and exit
      -h, --help            show this help message and exit
      -e CLUSTER_ID, --emrClusterId=CLUSTER_ID
                            Already existing EMR cluster to attach. When not provided it creates a new cluster.
      -s CLUSTER_SIZE, --clusterSize=CLUSTER_SIZE
                            Size of the cluster if needs to be created. See: emr.yaml
      -c CONFIG_FILE, --configFile=CONFIG_FILE
                            Config file provided externally, Default: emr.yaml
      -p PARAM_SET_NAME, --paramSetName=PARAM_SET_NAME
                            Parameter set name, See: emr.yaml
              
## Other helpful AWS commands

`aws emr list-clusters --active`

