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
    input_dir_10: str=None,
    input_dir_11: str=None,
    input_dir_12: str=None,
    input_dir_13: str=None,
    input_dir_14: str=None,
    input_dir_15: str=None,
    input_dir_16: str=None,
    input_dir_17: str=None,
    input_dir_18: str=None,
    input_dir_19: str=None,
    input_dir_20: str=None,
    input_dir_21: str=None,
    input_dir_22: str=None,
    input_dir_23: str=None,
    input_dir_24: str=None,
    input_dir_25: str=None,
    input_dir_26: str=None,
    input_dir_27: str=None,
    input_dir_28: str=None,
    input_dir_29: str=None,
    input_dir_30: str=None,
    input_dir_31: str=None,
    input_dir_32: str=None,
    input_dir_33: str=None,
    input_dir_34: str=None,
    input_dir_35: str=None,
    input_dir_36: str=None,
    input_dir_37: str=None,
    input_dir_38: str=None,
    input_dir_39: str=None,
    input_dir_40: str=None,
    input_dir_41: str=None,
    input_dir_42: str=None,
    input_dir_43: str=None,
    input_dir_44: str=None,
    input_dir_45: str=None,
    input_dir_46: str=None,
    input_dir_47: str=None,
    input_dir_48: str=None,
    input_dir_49: str=None,
    input_dir_50: str=None,
    input_dir_51: str=None,
    input_dir_52: str=None,
    input_dir_53: str=None,
    input_dir_54: str=None,
    input_dir_55: str=None,
    input_dir_56: str=None,
    input_dir_57: str=None,
    input_dir_58: str=None,
    input_dir_59: str=None,
):
    """
    Merges files from multiple input directories into a single output directory.
    Args:
        output_dir (str): The directory where merged files will be saved.
        input_dir_0 (str): The first input directory.
        input_dir_* (str): Additional input directories (up to 59).
    """
    input_dirs = [
        input_dir_0, input_dir_1, input_dir_2, input_dir_3, input_dir_4, input_dir_5, input_dir_6, input_dir_7, input_dir_8, input_dir_9, 
        input_dir_10, input_dir_11, input_dir_12, input_dir_13, input_dir_14, input_dir_15, input_dir_16, input_dir_17, input_dir_18, input_dir_19,
        input_dir_20, input_dir_21, input_dir_22, input_dir_23, input_dir_24, input_dir_25, input_dir_26, input_dir_27, input_dir_28, input_dir_29,
        input_dir_30, input_dir_31, input_dir_32, input_dir_33, input_dir_34, input_dir_35, input_dir_36, input_dir_37, input_dir_38, input_dir_39,
        input_dir_40, input_dir_41, input_dir_42, input_dir_43, input_dir_44, input_dir_45, input_dir_46, input_dir_47, input_dir_48, input_dir_49,
        input_dir_50, input_dir_51, input_dir_52, input_dir_53, input_dir_54, input_dir_55, input_dir_56, input_dir_57, input_dir_58, input_dir_59
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