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
        if LocalArtifactManager.BASE_DIR:
            os.makedirs(LocalArtifactManager.BASE_DIR, exist_ok=True)
            return os.path.join(LocalArtifactManager.BASE_DIR, f"{name}.pkl")
        else:
            return f"{name}.pkl"

    def save(self, artifact: Any, name: str) -> None:
        path = self._get_path(name)
        with open(path, 'wb') as f:
            if LocalArtifactManager.PICKLE:
                pickle.dump(artifact, f)
            else:
                joblib.dump(artifact, path)
        # print(f"{name} saved in: {path}")

    def load(self, name: str) -> Any:
        path = self._get_path(name)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Artifact '{name}' not found at {path}")
        with open(path, 'rb') as f:
            if LocalArtifactManager.PICKLE:
                artifact = pickle.load(f)
            else:
                artifact = joblib.load(f)
        # print(f"{name} loaded from: {path}")
        return artifact