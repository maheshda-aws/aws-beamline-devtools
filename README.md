
# AWSBeamlineDevtools
AWS Beamline Devtools is an environment users to perform interactive data analysis  using jupyter notebooks with Spark on EMR. This command line utility makes it easy for analysts and data engineers to manage dedicated or shared sandbox spark clusters on EMR. 

Users can attach their notebook instance with existing shared/dedicated EMR cluster or they can also create a new cluster by providing appropriate parameter. 

## Architecture Diagram
![Architecture](https://i.ibb.co/kKHdB6r/Beamline-Dev-Tools.jpg)

Please refer to: https://aws.amazon.com/blogs/machine-learning/build-amazon-sagemaker-notebooks-backed-by-spark-in-amazon-emr/


## Set up

 - As a prerequisite, please makesure the role associated with notebook instance has access to create new EMR cluster if you intent to create one.
 - Please make sure the security group attached with EMR master node allows traffice from the security group of the notebook instance.
 - Create a sagemaker notebook lifecycle configuration by copying the content from `notebook-lifecycle-config.sh`
 - Spin up a sagemaker notebook server using the above lifecycle config
 - Open the terminal once the server is ready

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

