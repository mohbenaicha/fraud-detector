import os
import joblib
import pickle
from typing import Any
from abc import ABC, abstractmethod

class ArtifactManagerInterface(ABC):
    @abstractmethod
    def save(self, artifact: Any, name: str) -> None:
        pass

    @abstractmethod
    def load(self, name: str) -> Any:
        pass


class LocalArtifactManager(ArtifactManagerInterface):
    BASE_DIR = "artifacts"
    PICKLE = True

    @staticmethod
    def _get_path(name: str) -> str:
        os.makedirs(LocalArtifactManager.BASE_DIR, exist_ok=True)
        return os.path.join(LocalArtifactManager.BASE_DIR, f"{name}.pkl")

    def save(self, artifact: Any, name: str) -> None:
        path = self._get_path(name)
        with open(path, 'wb') as f:
            if LocalArtifactManager.PICKLE:
                pickle.dump(artifact, f)
            else:
                joblib.dump(artifact, path)
        

    def load(self, name: str) -> Any:
        path = self._get_path(name)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Artifact '{name}' not found at {path}")
        with open(path, 'rb') as f:
            if LocalArtifactManager.PICKLE:
                artifact = pickle.load(f)
            else:
                artifact = joblib.load(f)
        
        return artifact

class VertexAIModelManager(ArtifactManagerInterface):
    def __init__(self, project_id: str, location: str, model_group: str, gcs_bucket: str):
        from google.cloud import aiplatform
        from google.cloud import storage
        self.aiplatform = aiplatform
        self.storage = storage
        self.project_id = project_id
        self.location = location
        self.model_group = model_group
        self.gcs_bucket = gcs_bucket
        aiplatform.init(project=project_id, location=location)
        self.storage_client = self.storage.Client(project=project_id)

    def set_version(self, version: str):
        self.version = version

    def save(self, artifact: str, name: str): # TODO: make signature match interface
        folder_blob_name = f"{self.model_group}/{self.version}/"
        blob = self.storage_client.bucket(self.gcs_bucket).blob(f"{folder_blob_name}{name}.pkl")

        if blob.exists():
            print(f"File {blob.name} already exists in GCS, skipping upload.")
        else:
            blob.upload_from_filename(artifact)
            print(f"Uploaded {artifact} to gs://{self.gcs_bucket}/{blob.name}")

        
        artifact_uri = f"gs://{self.gcs_bucket}/{folder_blob_name}"
        model = self.aiplatform.Model.upload(
            display_name=self.model_group,
            artifact_uri=artifact_uri,
            serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-5:latest",
        )
        print(f"Registered model in Vertex AI Model Registry: {model.resource_name}")
        return model

    def load(self, name: str):
        raise NotImplementedError("Loading models from Vertex AI Model Registry is not supported.")
    

class GCSArtifactManager(ArtifactManagerInterface):
    PICKLE = True
    GCS_BUCKET = "fraud_model_store"  

    def __init__(self, version: str = "1"):
        from google.cloud import storage
        self.version = version
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.GCS_BUCKET)
    
    @classmethod
    def set_pickle(cls, flag: bool):
        cls.PICKLE = flag

    def _get_blob(self, path: str) -> Any:
        blob_path = f"{path}/v{self.version}/model.pkl"
        return self.bucket.blob(blob_path)

    def save(self, artifact: Any, name: str) -> None:
        blob = self._get_blob(name)
        with blob.open("wb") as f:
            if self.PICKLE:
                pickle.dump(artifact, f)
            else:
                joblib.dump(artifact, f)
        print(f"Artifact saved to gs://{self.GCS_BUCKET}/{blob.name}")

    def load(self, name: str) -> Any:
        blob = self._get_blob(name)
        if not blob.exists():
            raise FileNotFoundError(f"Artifact not found at gs://{self.GCS_BUCKET}/{blob.name}")
        with blob.open("rb") as f:
            if self.PICKLE:
                artifact = pickle.load(f)
            else:
                artifact = joblib.load(f)
        return artifact