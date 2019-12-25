from typing import Optional, List, Dict, Any, Union, Collection
import logging
import json
import boto3

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class EMR:

    def __init__(self):
        self._session = boto3.Session()
        self._client_emr = boto3.Session().client(service_name="emr")


    @staticmethod
    def _build_cluster_args(**pars):

        args: Dict = {
            "Name": pars["cluster_name"],
            "LogUri": pars["logging_s3_path"],
            "ReleaseLabel": pars["emr_release"],
            "VisibleToAllUsers": pars["visible_to_all_users"],
            "JobFlowRole": pars["emr_ec2_role"],
            "ServiceRole": pars["emr_role"],
            "Instances": {
                "KeepJobFlowAliveWhenNoSteps": pars["keep_cluster_alive_when_no_steps"],
                "TerminationProtected": pars["termination_protected"],
                "Ec2SubnetId": pars["subnet_id"],
                "InstanceFleets": []
            },
            "EbsRootVolumeSize": pars["ebs_root_volume_size"],
            "StepConcurrencyLevel": pars["num_concurrent_steps"]
        }

        # EC2 Key Pair
        if pars["key_pair_name"] is not None:
            args["Instances"]["Ec2KeyName"] = pars["key_pair_name"]

        # Security groups
        if pars["security_group_master"] is not None:
            args["Instances"]["EmrManagedMasterSecurityGroup"] = pars["security_group_master"]
        if pars["security_groups_master_additional"] is not None:
            args["Instances"]["AdditionalMasterSecurityGroups"] = pars["security_groups_master_additional"]
        if pars["security_group_slave"] is not None:
            args["Instances"]["EmrManagedSlaveSecurityGroup"] = pars["security_group_slave"]
        if pars["security_groups_slave_additional"] is not None:
            args["Instances"]["AdditionalSlaveSecurityGroups"] = pars["security_groups_slave_additional"]
        if pars["security_group_service_access"] is not None:
            args["Instances"]["ServiceAccessSecurityGroup"] = pars["security_group_service_access"]

        # Configurations
        args["Configurations"]: List[Dict[str, Any]] = [{
            "Classification": "spark-log4j",
            "Properties": {
                "log4j.rootCategory": f"{pars['spark_log_level']}, console"
            }
        }]
        if pars["python3"]:
            args["Configurations"].append({
                "Classification":
                "spark-env",
                "Properties": {},
                "Configurations": [{
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    },
                    "Configurations": []
                }]
            })
        if pars["spark_glue_catalog"]:
            args["Configurations"].append({
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class":
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                },
                "Configurations": []
            })
        if pars["hive_glue_catalog"]:
            args["Configurations"].append({
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class":
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                },
                "Configurations": []
            })
        if pars["presto_glue_catalog"]:
            args["Configurations"].append({
                "Classification": "presto-connector-hive",
                "Properties": {
                    "hive.metastore.glue.datacatalog.enabled": "true"
                },
                "Configurations": []
            })
        if pars["maximize_resource_allocation"]:
            args["Configurations"].append({
                "Classification": "spark",
                "Properties": {
                    "maximizeResourceAllocation": "true"
                }
            })
        if (pars["spark_jars_path"] is not None) or (pars["spark_defaults"] is not None):
            spark_defaults: Dict[str, Union[str, Dict[str, str]]] = {
                "Classification": "spark-defaults",
                "Properties": {}
            }
            if pars["spark_jars_path"] is not None:
                spark_defaults["Properties"]["spark.jars"]: str = ",".join(pars["spark_jars_path"])
            if pars["spark_defaults"] is not None:
                for k, v in pars["spark_defaults"].items():
                    spark_defaults["Properties"][k]: str = v
            args["Configurations"].append(spark_defaults)

        # Applications
        if pars["applications"]:
            args["Applications"]: List[Dict[str, str]] = [{"Name": x} for x in pars["applications"]]

        # Bootstraps
        if pars["bootstraps_paths"]:
            args["BootstrapActions"]: List[Dict] = [{
                "Name": x,
                "ScriptBootstrapAction": {
                    "Path": x
                }
            } for x in pars["bootstraps_paths"]]

        # Debugging and Steps
        if (pars["debugging"] is True) or (pars["steps"] is not None):
            args["Steps"]: List[Dict[str, Collection[str]]] = []
            if pars["debugging"] is True:
                args["Steps"].append({
                    "Name": "Setup Hadoop Debugging",
                    "ActionOnFailure": "TERMINATE_CLUSTER",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["state-pusher-script"]
                    }
                })
            if pars["steps"] is not None:
                args["Steps"] += pars["steps"]

        # Master Instance Fleet
        timeout_action_master: str = "SWITCH_TO_ON_DEMAND" if pars[
            "spot_timeout_to_on_demand_master"] else "TERMINATE_CLUSTER"
        fleet_master: Dict = {
            "Name":
            "MASTER",
            "InstanceFleetType":
            "MASTER",
            "TargetOnDemandCapacity":
            pars["instance_num_on_demand_master"],
            "TargetSpotCapacity":
            pars["instance_num_spot_master"],
            "InstanceTypeConfigs": [
                {
                    "InstanceType": pars["instance_type_master"],
                    "WeightedCapacity": 1,
                    "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_master"],
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [{
                            "VolumeSpecification": {
                                "SizeInGB": pars["instance_ebs_size_master"],
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }],
                        "EbsOptimized":
                        True
                    },
                },
            ],
        }
        if pars["instance_num_spot_master"] > 0:
            fleet_master["LaunchSpecifications"]: Dict = {
                "SpotSpecification": {
                    "TimeoutDurationMinutes": pars["spot_provisioning_timeout_master"],
                    "TimeoutAction": timeout_action_master,
                }
            }
        args["Instances"]["InstanceFleets"].append(fleet_master)

        # Core Instance Fleet
        if (pars["instance_num_spot_core"] > 0) or pars["instance_num_on_demand_core"] > 0:
            timeout_action_core = "SWITCH_TO_ON_DEMAND" if pars[
                "spot_timeout_to_on_demand_core"] else "TERMINATE_CLUSTER"
            fleet_core: Dict = {
                "Name":
                "CORE",
                "InstanceFleetType":
                "CORE",
                "TargetOnDemandCapacity":
                pars["instance_num_on_demand_core"],
                "TargetSpotCapacity":
                pars["instance_num_spot_core"],
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": pars["instance_type_core"],
                        "WeightedCapacity": 1,
                        "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_core"],
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [{
                                "VolumeSpecification": {
                                    "SizeInGB": pars["instance_ebs_size_core"],
                                    "VolumeType": "gp2"
                                },
                                "VolumesPerInstance": 1
                            }],
                            "EbsOptimized":
                            True
                        },
                    },
                ],
            }
            if pars["instance_num_spot_core"] > 0:
                fleet_core["LaunchSpecifications"]: Dict = {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": pars["spot_provisioning_timeout_core"],
                        "TimeoutAction": timeout_action_core,
                    }
                }
            args["Instances"]["InstanceFleets"].append(fleet_core)

        # Task Instance Fleet
        if (pars["instance_num_spot_task"] > 0) or pars["instance_num_on_demand_task"] > 0:
            timeout_action_task: str = "SWITCH_TO_ON_DEMAND" if pars[
                "spot_timeout_to_on_demand_task"] else "TERMINATE_CLUSTER"
            fleet_task: Dict = {
                "Name":
                "TASK",
                "InstanceFleetType":
                "TASK",
                "TargetOnDemandCapacity":
                pars["instance_num_on_demand_task"],
                "TargetSpotCapacity":
                pars["instance_num_spot_task"],
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": pars["instance_type_task"],
                        "WeightedCapacity": 1,
                        "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_task"],
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [{
                                "VolumeSpecification": {
                                    "SizeInGB": pars["instance_ebs_size_task"],
                                    "VolumeType": "gp2"
                                },
                                "VolumesPerInstance": 1
                            }],
                            "EbsOptimized":
                            True
                        },
                    },
                ],
            }
            if pars["instance_num_spot_task"] > 0:
                fleet_task["LaunchSpecifications"]: Dict = {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": pars["spot_provisioning_timeout_task"],
                        "TimeoutAction": timeout_action_task,
                    }
                }
            args["Instances"]["InstanceFleets"].append(fleet_task)

        # Tags
        if pars["tags"] is not None:
            args["Tags"] = [{"Key": k, "Value": v} for k, v in pars["tags"].items()]

        logging.info(f"args: \n{json.dumps(args, default=str, indent=4)}")
        return args

    def create_cluster(self,
                       cluster_name: str,
                       logging_s3_path: str,
                       emr_release: str,
                       subnet_id: str,
                       emr_ec2_role: str,
                       emr_role: str,
                       num_concurrent_steps: int,
                       ebs_root_volume_size: int,
                       instance_type_master: str,
                       instance_type_core: str,
                       instance_type_task: str,
                       instance_ebs_size_master: int,
                       instance_ebs_size_core: int,
                       instance_ebs_size_task: int,
                       instance_num_on_demand_master: int,
                       instance_num_on_demand_core: int,
                       instance_num_on_demand_task: int,
                       instance_num_spot_master: int,
                       instance_num_spot_core: int,
                       instance_num_spot_task: int,
                       spot_bid_percentage_of_on_demand_master: int,
                       spot_bid_percentage_of_on_demand_core: int,
                       spot_bid_percentage_of_on_demand_task: int,
                       spot_provisioning_timeout_master: int,
                       spot_provisioning_timeout_core: int,
                       spot_provisioning_timeout_task: int,
                       spot_timeout_to_on_demand_master: bool = True,
                       spot_timeout_to_on_demand_core: bool = True,
                       spot_timeout_to_on_demand_task: bool = True,
                       python3: bool = True,
                       spark_glue_catalog: bool = True,
                       hive_glue_catalog: bool = True,
                       presto_glue_catalog: bool = True,
                       bootstraps_paths: Optional[List[str]] = None,
                       debugging: bool = True,
                       applications: Optional[List[str]] = None,
                       visible_to_all_users: bool = True,
                       key_pair_name: Optional[str] = None,
                       security_group_master: Optional[str] = None,
                       security_groups_master_additional: Optional[List[str]] = None,
                       security_group_slave: Optional[str] = None,
                       security_groups_slave_additional: Optional[List[str]] = None,
                       security_group_service_access: Optional[str] = None,
                       spark_log_level: str = "WARN",
                       spark_jars_path: Optional[List[str]] = None,
                       spark_defaults: Dict[str, str] = None,
                       maximize_resource_allocation: bool = False,
                       steps: Optional[List[Dict[str, Collection[str]]]] = None,
                       keep_cluster_alive_when_no_steps: bool = True,
                       termination_protected: bool = False,
                       tags: Optional[Dict[str, str]] = None):
        """
        Create an EMR cluster using instance fleet configurations

        Arguments:
            cluster_name {str} -- Cluster name
            logging_s3_path {str} -- Logging s3 path (e.g. s3://BUCKET_NAME/DIRECTORY_NAME/)
            emr_release {str} -- EMR release (e.g. emr-5.28.0)
            subnet_id {str} -- VPC subnet ID
            emr_ec2_role {str} -- IAM role name for cluster EC2 instance (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles.html)
            emr_role {str} -- IAM role name for EMR (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles.html)
            num_concurrent_steps {int} -- Number of concurrent steps to execute
            ebs_root_volume_size {int} -- Root volume size in GB
            instance_type_master {str} -- Type of master node (see EC2 instance types)
            instance_type_core {str} -- Type of core node (see EC2 instance types)
            instance_type_task {str} -- Type of task node (see EC2 instance types)
            instance_ebs_size_master {int} -- EBS size on master node in GB
            instance_ebs_size_core {int} -- EBS size on core node in GB
            instance_ebs_size_task {int} -- EBS size on task node in GB
            instance_num_on_demand_master {int} -- Number of master nodes on demand
            instance_num_on_demand_core {int} -- Number of core nodes on demand
            instance_num_on_demand_task {int} -- Number of task nodes on demand
            instance_num_spot_master {int} -- Number of master nodes on spot pricing
            instance_num_spot_core {int} -- Number of core nodes on spot pricing
            instance_num_spot_task {int} -- Number of task nodes on spot pricing
            spot_bid_percentage_of_on_demand_master {int} -- Spot bid price as percentage of on demand pricing for master node
            spot_bid_percentage_of_on_demand_core {int} -- Spot bid price as percentage of on demand pricing for core node
            spot_bid_percentage_of_on_demand_task {int} -- Spot bid price as percentage of on demand pricing for task node
            spot_provisioning_timeout_master {int} -- The spot provisioning timeout period in minutes for master node(between 5 to 1440). 
            spot_provisioning_timeout_core {int} -- The spot provisioning timeout period in minutes for core node (between 5 to 1440). 
            spot_provisioning_timeout_task {int} --The spot provisioning timeout period in minutes for task node (between 5 to 1440). 

        Keyword Arguments:
            spot_timeout_to_on_demand_master {bool} -- After timeout of spot bidding fall back to on demand? (default: {True})
            spot_timeout_to_on_demand_core {bool} -- After timeout of spot bidding fall back to on demand?  (default: {True})
            spot_timeout_to_on_demand_task {bool} -- After timeout of spot bidding fall back to on demand?  (default: {True})
            python3 {bool} -- Python3 default enabled (default: {True})
            spark_glue_catalog {bool} -- Spark on glue catalog enabled? (default: {True})
            hive_glue_catalog {bool} -- Hive on glue catalog enabled? (default: {True})
            presto_glue_catalog {bool} -- Presto on glue catalog enabled? (default: {True})
            bootstraps_paths {Optional[List[str]]} -- Bootstrap paths in s3 (e.g ["s3://<bucket_name>/script.sh"]) (default: {None})
            debugging {bool} -- Debugging enabled? (default: {True})
            applications {Optional[List[str]]} -- List of application to be included (default: {None})
            visible_to_all_users {bool} -- Is cluster visible to all users? (default: {True})
            key_pair_name {Optional[str]} -- Key pair name (default: {None})
            security_group_master {Optional[str]} -- Master  security group (default: {None})
            security_groups_master_additional {Optional[List[str]]} -- Additional security group for master (default: {None})
            security_group_slave {Optional[str]} -- Slave security group (default: {None})
            security_groups_slave_additional {Optional[List[str]]} -- Additional security group for slave (default: {None})
            security_group_service_access {Optional[str]} -- Amazon EC2 security group for the Amazon EMR service to access clusters in VPC private subnets. (default: {None})
            spark_log_level {str} -- Log level(default: {"WARN"})
            spark_jars_path {Optional[List[str]]} -- Spark jar in s3 (default: {None})
            spark_defaults {Dict[str, str]} -- Spark defaults (default: {None})
            maximize_resource_allocation {bool} -- Configure your executors to utilize the maximum resources possible? (default: {False})
            steps {Optional[List[Dict[str, Collection[str]]]]} -- Steps to execute(default: {None})
            keep_cluster_alive_when_no_steps {bool} -- Keep cluster alive when no steps executed? (default: {True})
            termination_protected {bool} -- Termination protection enabled? (default: {False})
            tags {Optional[Dict[str, str]]} -- Tags(default: {None})

        Returns:
            Dictionary -- Response from emr run_job_flow API
        """ 

        args = EMR._build_cluster_args(**locals())
        response = self._client_emr.run_job_flow(**args)
        logging.info(f"Response: \n{json.dumps(response, default=str, indent=4)}")
        return response

    def get_cluster_state(self, cluster_id: str) -> str:

        """
        Get state of a cluster given its cluster id  

        Arguments:
            cluster_id {str} -- JobflowId

        Returns:
            str -- State of cluster like WAITING, RUNNING, STARTING etc.
        """
        response: Dict = self._client_emr.describe_cluster(ClusterId=cluster_id)
        logging.info(f"Response: \n{json.dumps(response, default=str, indent=4)}")
        return response["Cluster"]["Status"]["State"]

    def get_cluster_description(self, cluster_id: str):
        """
        Describe cluster for given JobFlowId

        Arguments:
            cluster_id {str} -- JobFlowId

        Returns:
            Dictionary -- Response to describe cluster API
        """
        response: Dict = self._client_emr.describe_cluster(ClusterId=cluster_id)
        logging.info(f"Response: \n{json.dumps(response, default=str, indent=4)}")
        return response

    def get_cluster_instances(self, cluster_id: str, instance_group_types: List=["MASTER"]):
        """
        Get instance details of an EMR cluster

        Arguments:
            cluster_id {str} -- JobFlowId

        Keyword Arguments:
            instance_group_types {List} -- Type if instance (default: {["MASTER"]})

        Returns:
            Dictionary-- Response of list_instances API
        """
        response: Dict = self._client_emr.list_instances(ClusterId=cluster_id, InstanceGroupTypes=instance_group_types)
        logging.info(f"Response: \n{json.dumps(response, default=str, indent=4)}")
        return response

    def terminate_cluster(self, cluster_id: str) -> None:
        """
        Terminate an EMR cluster.

        Arguments:
            cluster_id {str} -- JobFlowId
        
        Returns:
            Dictionary-- Response of terminate_job_flows API
        """
        response: Dict = self._client_emr.terminate_job_flows(JobFlowIds=[
            cluster_id,
        ])
        logging.info(f"Response: \n{json.dumps(response, default=str, indent=4)}")
        return response