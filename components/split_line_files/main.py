# split lines to multiple datasets, each output dataset maintain the exact same directory structure as input

import glob
from itertools import cycle
import json
import os
from typing import List
import logging
import fire

logging.basicConfig(
    format="[%(prefix)s] %(levelname)s %(message)s",
    level=logging.INFO,
)

_base_logger = logging.getLogger(__name__)

def glob_files(dir: str, glob_patterns: str, recursive: bool=False) -> List[str]:
    files = set()
    for pattern in glob_patterns.split('|'):
        files.update(glob.glob(os.path.join(dir, pattern), recursive=recursive))

    files = [f for f in files if os.path.isfile(f)]

    return sorted(list(files))

def process_file(
    input_dir: str,
    output_dirs: List[str],
    relative_path: str,
):
    logger = logging.LoggerAdapter(_base_logger, {'prefix': f'[{relative_path}]'})
    input_file = os.path.join(input_dir, relative_path)
    output_files = []
    for output_dir in output_dirs:
        output_file = os.path.join(output_dir, relative_path)
        output_subdir = os.path.dirname(output_file)
        os.makedirs(output_subdir, exist_ok=True)
        output_files.append(output_file)
    
    output_fps = []
    for i, output_file in enumerate(output_files):
        fp = open(output_file, 'w', encoding='utf-8')
        output_fps.append((i, fp))
    
    output_fp_cycle = cycle(output_fps)
    counts = {i: 0 for i in range(len(output_dirs))}
    with open(input_file, 'r', encoding='utf-8') as fin:
        for i, line in enumerate(fin):
            output_fp_idx, output_fp = next(output_fp_cycle)
            output_fp.write(line)
            counts[output_fp_idx] += 1
            if (i + 1) % 10000 == 0:
                logger.info(f'Processed {i + 1} lines')

    logger.info(f'Finished processing file {relative_path}, total lines: {i + 1}, counts: {counts}')
    for _, fp in output_fps:
        fp.close()
    # write counts to report file
    for i, output_dir in enumerate(output_dirs):
        report_file = os.path.join(output_dir, relative_path.replace('.jsonl', '_report.json'))
        report = {
            'relative_path': relative_path,
            'count': counts[i],
        }
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, fp=f, indent=4, ensure_ascii=False)
    logger.info(f'Report files written')
    return counts


def main(
    input_dir: str,
    glob_patterns: str,
    n_splits: int,
    output_dir_0: str,
    output_dir_1: str,
    output_dir_2: str,
    output_dir_3: str,
    output_dir_4: str,
):
    logger = logging.LoggerAdapter(_base_logger, {'prefix': '[main]'})
    output_dirs = [output_dir_0, output_dir_1, output_dir_2, output_dir_3, output_dir_4]
    if n_splits < 2 or n_splits > len(output_dirs):
        raise ValueError(f"n_splits should be between 2 and {len(output_dirs)}, got {n_splits}")
    output_dirs = output_dirs[:n_splits]
    logger.info(f"{len(output_dirs)} output dirs: {output_dirs}")

    input_files = glob_files(dir=input_dir, glob_patterns=glob_patterns, recursive=True)
    logger.info(f"Found {len(input_files)} input files in {input_dir} with patterns {glob_patterns}")

    args = []
    for input_file in input_files:
        relative_path = os.path.relpath(input_file, input_dir)
        args.append((input_dir, output_dirs, relative_path))

    n_proc = min(os.cpu_count(), len(args))
    file_split_counts = []
    if n_proc <= 1:
        for arg in args:
            file_split_counts.append(process_file(*arg))
    else:
        from multiprocessing import Pool
        with Pool(processes=n_proc) as pool:
            file_split_counts.extend(pool.starmap(process_file, args))
    logger.info("All files processed.")
    # aggregate counts
    total_counts = {i: 0 for i in range(len(output_dirs))}
    for split_counts in file_split_counts:
        for i in range(len(output_dirs)):
            total_counts[i] += split_counts.get(i, 0)
    logger.info(f"Total counts per output dir: {total_counts}")
    for i, output_dir in enumerate(output_dirs):
        report_file = os.path.join(output_dir, 'total_report.json')
        report = {
            'total_count': total_counts[i],
        }
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, fp=f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    fire.Fire(main)