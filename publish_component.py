
import os
import shutil
from azure.ai.ml import load_component
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
import fire
import yaml

    
def main(
        config_file: str, 
        key_config: str = "wus2", # -k, key in aml_config.yaml, default is "wus2"
        src_path: str = "./src", # -s, relative path to src folder, default is "src"
    ):

    with open("./aml_config.yaml", 'r') as file:
        aml_config = yaml.safe_load(file)

    subscription_id, resource_group, target = aml_config[key_config]['subscription_id'], aml_config[key_config]['resource_group'], aml_config[key_config]['target']

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

    no_src = src_path is None or src_path.lower() == "none"
    
    if not no_src:
        print("no_src is False, will copy src folder to component folder")
        local_src_abs_path = os.path.abspath(src_path)
        if not os.path.exists(local_src_abs_path) or not os.path.isdir(local_src_abs_path):
            raise RuntimeError(f"src folder does not exist: {local_src_abs_path}")

        comp_dir = os.path.dirname(config_file)
        print(f"component directory: {comp_dir}")

        target_src_dir = os.path.join(comp_dir, "src")
        if os.path.exists(target_src_dir) and os.path.isdir(target_src_dir):
            raise RuntimeError(f"target src folder already exists: {target_src_dir}. Please remove it.")
            
        shutil.copytree(src=local_src_abs_path, dst=target_src_dir, ignore=shutil.ignore_patterns("*.pyc", "__pycache__", ".git"))
    
    print("load and pubish component")
    component = load_component(config_file)
    component = ml_client.components.create_or_update(component)

    if not no_src:
        print("clean up component folder")
        shutil.rmtree(os.path.join(comp_dir, "src"))


if __name__ == "__main__":
    fire.Fire(main)