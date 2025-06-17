# import required libraries
import os
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Environment
from azure.identity import DefaultAzureCredential
import fire
import yaml

def main(
    config_file: str, # -c, absolute path, .env.yaml file and .conda.yaml file must be there
):
    with open("./aml_config.yaml", 'r') as file:
        aml_config = yaml.safe_load(file)

    subscription_id, resource_group, target = aml_config['subscription_id'], aml_config['resource_group'], aml_config['target']
    # Create a credential object using DefaultAzureCredential
    credential = DefaultAzureCredential()
    target = target.split(":")
    if target[0] == "ws":
        workspace_name = target[1]
        print(f"Target is workspace: {workspace_name}")
        ml_client = MLClient(
            credential=credential,
            subscription_id=subscription_id,
            resource_group_name=resource_group,
            workspace_name=workspace_name,
        )
    elif target[0] == "reg":
        registry_name = target[1]
        print(f"Target is registry: {registry_name}")
        ml_client = MLClient(
            credential=credential,
            registry_name=registry_name,)
    else:
        raise ValueError(f"Invalid target: {target}")
        
    # load the configuration from the YAML file
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    print(f"config: {config}")
    
    name, version, image, conda_file, description = config['name'], config['version'], config['image'], config['conda_file'], config['description']
    # conda_file is relative path now, convert to absolute path
    conda_file = os.path.abspath(os.path.join(os.path.dirname(config_file), conda_file))
    print(f"conda_file: {conda_file}")
    env_docker = Environment(
        image=image,
        conda_file=conda_file,
        name=name,
        version=version,
        description=description,
    )

    print(f"create_or_update...")
    ml_client.environments.create_or_update(env_docker)
    print(f"done")
    
if __name__ == "__main__":
    fire.Fire(main)