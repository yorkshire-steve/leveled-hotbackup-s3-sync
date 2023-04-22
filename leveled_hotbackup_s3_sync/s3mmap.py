from typing import Union

import s3fs


class S3FileReader:
    def __init__(self, path: str, endpoint: Union[str, None] = None):
        s3_filesystem = s3fs.S3FileSystem(anon=False, endpoint_url=endpoint)
        self.handle = s3fs.S3File(s3_filesystem, path.lstrip("s3://"), "rb", fill_cache=False)
        self.info = self.handle.info()

    def close(self):
        try:
            self.handle.close()
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def __len__(self):
        return self.info["size"]

    def __getitem__(self, val):
        start, end = 0, self.info["size"]
        if isinstance(val, slice):
            if val.start:
                if val.start < 0:
                    raise NotImplementedError
                start = val.start
            if val.stop:
                if val.stop < 0:
                    raise NotImplementedError
                end = val.stop
            if val.step:
                raise NotImplementedError
        else:
            start, end = val, val + 1
        self.handle.seek(start)
        return self.handle.read(end - start)
