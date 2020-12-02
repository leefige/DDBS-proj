class HDFS_API(object):
    def setup(self, dirpath, **kwargs):
        raise NotImplementedError

    def put_file(self, src_path: str, dst_path: str, **kwargs):
        raise NotImplementedError

    def put_files(self, src_paths: list, dst_dir: str, **kwargs):
        raise NotImplementedError

    def put_dir(self, src_path: str, dst_dir: str, **kwargs):
        raise NotImplementedError

    def get_file(self, src_path: str, dst_path: str, **kwargs):
        raise NotImplementedError

    def get_dir(self, src_path: str, dst_dir: str, **kwargs):
        raise NotImplementedError
