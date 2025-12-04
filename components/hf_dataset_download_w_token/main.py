import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import fire
import subprocess

def main(
    output_dir: str,
    dataset_name: str,
    keyvault_url: str,
    hf_token_key: str,
    aml_run: bool = False,
):
    if aml_run:
        credential = DefaultAzureCredential()#AzureMLOnBehalfOfCredential()
        secret_client = SecretClient(vault_url=keyvault_url, credential=credential)
        hf_token = secret_client.get_secret(hf_token_key).value
        print(f"Retrieved Hugging Face token from Key Vault: {hf_token_key[:5]}... (truncated)")
    else:
        hf_token = os.environ['HF_TOKEN']
        print(f"Retrieved Hugging Face token from environment variable: {hf_token[:5]}... (truncated)")

    if hf_token is None or len(hf_token) == 0:
        raise ValueError("hf_token is none or empty!")

    # subprocess.run(
    #     ["huggingface-cli", "download", dataset_name, "--repo-type", "dataset", "--local-dir", output_dir, "--token", hf_token])
    subprocess.run(
        ["hf", "download", dataset_name, "--repo-type", "dataset", "--local-dir", output_dir, "--token", hf_token])
    
    print(f"Download done.")
    

if __name__ == "__main__":
    fire.Fire(main)