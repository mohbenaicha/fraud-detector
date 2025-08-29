
from abc import ABC, abstractmethod
import pickle
import boto3
from typing import Any

class ArtifactManagerInterface(ABC):
    @abstractmethod
    def save(self, artifact: Any, name: str) -> None:
        pass

    @abstractmethod
    def load(self, name: str) -> Any:
        pass


class S3ArtifactManager(ArtifactManagerInterface):
    """
    Usage:\n
    artifact_manager = S3ArtifactManager(bucket_name="fraud-model-store")\n
    artifact_manager.save(fitted_ohe, "fitted_ohe.pkl")\n
    ohe = artifact_manager.load("fitted_ohe.pkl")\n

    """
    def __init__(self, bucket_name: str, prefix: str = ""):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name
        self.prefix = prefix.rstrip("/")

    def _full_key(self, name: str) -> str:
        return f"{self.prefix}/{name}" if self.prefix else name

    def save(self, artifact: Any, name: str) -> None:
        key = self._full_key(name)
        serialized = pickle.dumps(artifact)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=serialized)

    def load(self, name: str) -> Any:
        key = self._full_key(name)
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pickle.loads(obj["Body"].read())
