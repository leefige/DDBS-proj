import os

import pyhdfs
from .hdfsapi import HDFS_API


class WebHDFS(HDFS_API):
    def __init__(self, host: str = "localhost", port: int = 9870, user_name="root", **kwargs):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user_name
        self.home = f"/user/{self.user:s}"
        self.fs = pyhdfs.HdfsClient(
            hosts=[f"{host:s}:{port:d}", "localhost:9870"], user_name=user_name, **kwargs)

    def __abs_path(self, path: str):
        if isinstance(path, str):
            if len(path) > 0 and path[0] != '/':
                path = f"{self.home}/{path}"
        return path

    def setup(self, dirpath, **kwargs):
        self.put_dir(dirpath, ".")

    def put_file(self, src_path: str, dst_path: str, exist_ok=False, check=True, **kwargs):
        if check:
            if not isinstance(src_path, str):
                raise TypeError(f"Error: expect str, get {type(src_path)}")

            if not os.path.exists(src_path):
                raise RuntimeError(f"Error: file not exists: '{src_path:s}'")

        dst_path = f"{self.__abs_path(dst_path)}"

        if exist_ok:
            try:
                self.fs.copy_from_local(src_path, dst_path)
            except pyhdfs.HdfsFileAlreadyExistsException:
                pass
        else:
            self.fs.copy_from_local(src_path, dst_path)
        return

    def put_files(self, src_paths: list, dst_dir: str, exist_ok=False, **kwargs):
        if not isinstance(src_paths, list):
            raise TypeError(f"Error: expect list, get {type(src_paths)}")

        for src_path in src_paths:
            if not isinstance(src_path, str):
                raise TypeError(f"Error: expect str, get {type(src_path)}")
            if not os.path.exists(src_path):
                raise RuntimeError(f"Error: file not exists: '{src_path:s}'")

        dst_dir = f"{self.__abs_path(dst_dir)}"

        for src_path in src_paths:
            self.put_file(
                src_path, f"{dst_dir}/{os.path.basename(src_path)}", exist_ok=exist_ok, check=False)
        return

    def put_dir(self, src_path: str, dst_dir: str, **kwargs):
        if not isinstance(src_path, str):
            raise TypeError(f"Error: expect str, get {type(src_path)}")

        if not os.path.exists(src_path):
            raise RuntimeError(f"Error: dir not exists: '{src_path:s}'")

        if not os.path.isdir(src_path):
            raise RuntimeError(f"Error: '{src_path:s}' is not a dir")

        dst_path_ = f"{self.__abs_path(dst_dir)}"
        pref_cnt = len(src_path.split('/')) - 1
        basename = os.path.basename(src_path)
        if self.fs.exists(f"{dst_path_}/{basename}"):
            raise FileExistsError(
                f"Error: dir already exists: '{dst_path_}/{basename}'")

        for dirname, subdir_list, file_list in os.walk(src_path):
            relative_dir = '/'.join(dirname.split('/')[pref_cnt:])
            self.fs.mkdirs(f"{dst_path_}/{relative_dir}")
            for fname in file_list:
                self.put_file(os.path.join(dirname, fname),
                              f"{dst_path_}/{relative_dir}/{fname}")
        return

    def get_file(self, src_path: str, dst_path: str, check=True, **kwargs):
        if check:
            if not isinstance(src_path, str):
                raise TypeError(f"Error: expect str, get {type(src_path)}")

            if os.path.exists(dst_path) and not os.path.isdir(dst_path):
                raise RuntimeError(
                    f"Error: file already exists: '{dst_path:s}'")

        src_path = self.__abs_path(src_path)

        self.fs.copy_to_local(self.__abs_path(src_path), dst_path)
        return

    def get_dir(self, src_path: str, dst_dir: str, **kwargs):
        if not isinstance(src_path, str):
            raise TypeError(f"Error: expect str, get {type(src_path)}")

        if not isinstance(dst_dir, str):
            raise TypeError(f"Error: expect str, get {type(dst_dir)}")

        basename = os.path.basename(src_path)
        if os.path.exists(os.path.join(dst_dir, basename)):
            raise FileExistsError(
                f"Error: dir already exists: '{os.path.join(dst_dir, basename)}'")

        src_path = f"{self.__abs_path(src_path)}"
        pref_cnt = len(src_path.split('/')) - 1

        for dirname, subdir_list, file_list in self.fs.walk(src_path):
            relative_dir = '/'.join(dirname.split('/')[pref_cnt:])
            os.mkdir(os.path.join(dst_dir, relative_dir))
            for fname in file_list:
                self.get_file(f"{dirname}/{fname}",
                              os.path.join(dst_dir, relative_dir, fname))
        return


if __name__ == "__main__":
    fs = WebHDFS(host="hd-name")
    # fs.setup()
    fs.put_files(["hdfs.py"], "articles", exist_ok=True)
    fs.put_files(["hdfs.py", "webhdfs.py"], "articles", exist_ok=True)
    os.makedirs("tmp_a", exist_ok=True)
    fs.get_dir("articles", "tmp_a")
    fs.put_dir("tmp_a", "articles", exist_ok=False)
