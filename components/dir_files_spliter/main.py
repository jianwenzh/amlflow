import glob
import os
import shutil
from typing import List
import fire

def glob_files(dir: str, glob_patterns: str) -> List[str]:
    files = []
    for pattern in glob_patterns.split('|'):
        files.extend(glob.glob(os.path.join(dir, pattern)))

    return files

def main(
    input_dir: str,
    n_splits: int,
    output_dir_0: str,
    glob_patterns: str="*",
    output_dir_1: str=None,
    output_dir_2: str=None,
    output_dir_3: str=None,
    output_dir_4: str=None,
    output_dir_5: str=None,
    output_dir_6: str=None,
    output_dir_7: str=None,
    output_dir_8: str=None,
    output_dir_9: str=None,
):
    """
    Splits files in the input directory into multiple output directories.

    Args:
        input_dir (str): The directory containing files to split.
        n_splits (int): The number of splits to create.
        output_dir_0 (str): The first output directory.
        glob_patterns (str, optional): Glob patterns to filter files. Defaults to None, which matches all files.
        output_dir_1 (str, optional): The second output directory. Defaults to None.
        output_dir_2 (str, optional): The third output directory. Defaults to None.
        output_dir_3 (str, optional): The fourth output directory. Defaults to None.
        output_dir_4 (str, optional): The fifth output directory. Defaults to None.
        output_dir_5 (str, optional): The sixth output directory. Defaults to None.
        output_dir_6 (str, optional): The seventh output directory. Defaults to None.
        output_dir_7 (str, optional): The eighth output directory. Defaults to None.
        output_dir_8 (str, optional): The ninth output directory. Defaults to None.
        output_dir_9 (str, optional): The tenth output directory. Defaults to None.
    """
    # Implementation of the file splitting logic goes here
    output_dirs = [
        output_dir_0, output_dir_1, output_dir_2, output_dir_3,
        output_dir_4, output_dir_5, output_dir_6, output_dir_7,
        output_dir_8, output_dir_9
    ]
    output_dirs = [d for d in output_dirs if d is not None]
    print(f"Number of output directories: {len(output_dirs)}")

    if n_splits > len(output_dirs):
        raise ValueError(f"n_splits ({n_splits}) must not exceed the number of output directories ({len(output_dirs)})")
    
    files = sorted(glob_files(input_dir, glob_patterns))

    # spread files across output directories
    for i, file in enumerate(files):
        output_dir = output_dirs[i % n_splits]
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, os.path.basename(file))
        shutil.copy(file, output_file_path)

    for output_dir in output_dirs:
        files = os.listdir(output_dir)
        if not files:
            print(f"Warning: {output_dir} is empty after splitting files.")
        else:
            print(f"{len(files)} Files in {output_dir}: {files}")
    

if __name__ == "__main__":
    fire.Fire(main)