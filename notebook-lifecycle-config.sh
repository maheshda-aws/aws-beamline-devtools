#!/bin/bash
#Build the package for distribution
git clone https://github.com/maheshda-aws/aws-beamline-devtools.git
cd /aws-beamline-devtools && /home/ec2-user/anaconda3/envs/JupyterSystemEnv/bin/python setup.py bdist_wheel

#Upgrade dependency and install package
sudo -u ec2-user -i <<'EOF'

source activate JupyterSystemEnv
pip install --upgrade --quiet awscli
pip install --upgrade --quiet boto3
pip install --upgrade --quiet /aws-beamline-devtools/dist/aws_beamline_devtools-0.0.1-py3-none-any.whl
source deactivate

EOF

mv /aws-beamline-devtools/attach_emr.py /home/ec2-user/attach-emr
mv /aws-beamline-devtools/emr.yaml /home/ec2-user/emr.yaml
chmod +x /home/ec2-user/attach-emr

echo "Set up is successfully completed. You can now attach EMR using './attach-emr' commandline. Please use './attach_emr -h' for help on cli options"
