import os
import shutil
import fire

def main(
    output_dir: str,
    input_dir_0: str,
    input_dir_1: str=None,
    input_dir_2: str=None,
    input_dir_3: str=None,
    input_dir_4: str=None,
    input_dir_5: str=None,
    input_dir_6: str=None,
    input_dir_7: str=None,
    input_dir_8: str=None,
    input_dir_9: str=None,
):
    """
    Merges files from multiple input directories into a single output directory.
    Args:
        output_dir (str): The directory where merged files will be saved.
        input_dir_0 (str): The first input directory.
        input_dir_1 (str, optional): The second input directory. Defaults to None.
        input_dir_2 (str, optional): The third input directory. Defaults to None.
        input_dir_3 (str, optional): The fourth input directory. Defaults to None.
        input_dir_4 (str, optional): The fifth input directory. Defaults to None.
        input_dir_5 (str, optional): The sixth input directory. Defaults to None.
        input_dir_6 (str, optional): The seventh input directory. Defaults to None.
        input_dir_7 (str, optional): The eighth input directory. Defaults to None.
        input_dir_8 (str, optional): The ninth input directory. Defaults to None.
        input_dir_9 (str, optional): The tenth input directory. Defaults to None.
    """
    input_dirs = [
        input_dir_0, input_dir_1, input_dir_2, input_dir_3,
        input_dir_4, input_dir_5, input_dir_6, input_dir_7,
        input_dir_8, input_dir_9
    ]
    input_dirs = [d for d in input_dirs if d is not None]

    for i, input_dir in enumerate(input_dirs):
        print(f"Processing input directory {i + 1}/{len(input_dirs)}: {input_dir}")
        files = os.listdir(input_dir)
        if files:
            print(f"Copying files from {input_dir} to {output_dir}")
            for file in files:
                path = os.path.join(input_dir, file)
                if os.path.isfile(path):
                    # Copy the file to the output directory
                    print(f"Copying file {file} to {output_dir}")
                    shutil.copy(path, os.path.join(output_dir, file))
                elif os.path.isdir(path):
                    # If it's a directory, copy the entire directory
                    print(f"Copying directory {file} to {output_dir}")
                    shutil.copytree(path, os.path.join(output_dir, file), dirs_exist_ok=True)

        else:
            print(f"No items found in {input_dir}, skipping.")

if __name__ == "__main__":
    fire.Fire(main)