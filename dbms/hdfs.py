import subprocess
import sys
import os

# hdfs dfs -mkdir -p articles
# hdfs dfs -moveFromLocal ./articles/* articles


def subcall(cmd, show_output=False, **kwargs):
    try:
        output = subprocess.check_output(cmd, shell=True, **kwargs)
    except subprocess.CalledProcessError as e:
        print(f"Error exec '{cmd:s}', return code {e.returncode}, outputs:")
        print(e.output)
        return False

    if show_output:
        print(output)
    return True


def setup():
    # move videos and images to HDFS
    assert subcall("hdfs dfs -mkdir -p articles")
    assert subcall("hdfs dfs -moveFromLocal ./articles/* articles")


def put_files(src_paths, dst_path):
    if isinstance(src_paths, str):
        src_paths = [src_paths]

    if not isinstance(src_paths, list):
        raise TypeError(f"Error: expect str or list, get {type(src_paths)}")

    for src_path in src_paths:
        if not os.path.exists(src_path):
            raise RuntimeError(f"Error: file not exists: '{src_path:s}'")
        # elif os.path.isdir(src_path):
        #     raise RuntimeError(f"Error: '{src_path}' is a dir")

    return subcall(f"hdfs dfs -put {' '.join(src_paths):s} {dst_path:s}")


def get_file(src_path, dst_path):
    if not isinstance(src_path, str):
        raise TypeError(f"Error: expect str, get {type(src_path)}")

    if os.path.exists(dst_path) and not os.path.isdir(dst_path):
        raise RuntimeError(f"Error: file already exists: '{dst_path:s}'")

    return subcall(f"hdfs dfs -get {src_path:s} {dst_path:s}")
