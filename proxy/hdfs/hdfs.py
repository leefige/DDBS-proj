import os
import subprocess
import sys

from .hdfsapi import HDFS_API


class HDFS(HDFS_API):
    def __init__(self):
        super().__init__()

    def __subcall(self, cmd, show_output=False, **kwargs):
        try:
            output = subprocess.check_output(cmd, shell=True, **kwargs)
        except subprocess.CalledProcessError as e:
            print(
                f"Error exec '{cmd:s}', return code {e.returncode}, outputs:")
            print(e.output)
            return False

        if show_output:
            print(output)
        return True

    def setup(self, dirpath, **kwargs):
        # move videos and images to HDFS
        assert self.__subcall(f"hdfs dfs -mkdir -p articles")
        assert self.__subcall(f"hdfs dfs -copyFromLocal {dirpath}/* articles")

    def __put(self, src_paths, dst_path):
        for src_path in src_paths:
            if not os.path.exists(src_path):
                raise RuntimeError(f"Error: file not exists: '{src_path:s}'")
            # elif os.path.isdir(src_path):
            #     raise RuntimeError(f"Error: '{src_path}' is a dir")

        return self.__subcall(f"hdfs dfs -put {' '.join(src_paths):s} {dst_path:s}")

    def put_file(self, src_path: str, dst_path: str, **kwargs):
        if not isinstance(src_path, str):
            raise TypeError(f"Error: expect str, get {type(src_path)}")
        return self.__put([src_path], dst_path)

    def put_files(self, src_paths, dst_path, **kwargs):
        if not isinstance(src_paths, list):
            raise TypeError(f"Error: expect list, get {type(src_paths)}")
        return self.__put(src_paths, dst_path)

    def put_dir(self, src_path: str, dst_dir: str, **kwargs):
        if not isinstance(src_path, str):
            raise TypeError(f"Error: expect str, get {type(src_path)}")

        if not os.path.exists(src_path):
            raise RuntimeError(f"Error: dir not exists: '{src_path:s}'")

        if not os.path.isdir(src_path):
            raise RuntimeError(f"Error: '{src_path:s}' is not a dir")
        return self.put_file(src_path, dst_dir)

    def get_file(self, src_path, dst_path, **kwargs):
        if not isinstance(src_path, str):
            raise TypeError(f"Error: expect str, get {type(src_path)}")

        if os.path.exists(dst_path) and not os.path.isdir(dst_path):
            raise RuntimeError(f"Error: file already exists: '{dst_path:s}'")

        return self.__subcall(f"hdfs dfs -get {src_path:s} {dst_path:s}")

    def get_dir(self, src_path: str, dst_dir: str, **kwargs):
        if not isinstance(src_path, str):
            raise TypeError(f"Error: expect str, get {type(src_path)}")

        if not isinstance(dst_dir, str):
            raise TypeError(f"Error: expect str, get {type(dst_dir)}")

        return self.get_file(src_path, dst_dir)


if __name__ == "__main__":
    fs = HDFS()
    # fs.setup()
    fs.put_files(["hdfs.py"], "articles", exist_ok=True)
    fs.put_files(["hdfs.py", "webhdfs.py"], "articles", exist_ok=True)
    os.makedirs("tmp_a", exist_ok=True)
    fs.get_dir("articles", "tmp_a")
    fs.put_dir("tmp_a", "articles", exist_ok=False)
