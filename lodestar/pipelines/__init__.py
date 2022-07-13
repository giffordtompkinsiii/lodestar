from abc import ABC
from typing import List, Dict


class Pipeline(ABC):
    @classmethod
    def extract(cls) -> dict:
        return dict()

    @classmethod
    def transform(cls) -> List[Dict]:
        return dict()

    @classmethod
    def load(cls) -> None:
        return None

    # @classmethod
    def run_pipeline(self):
        self.extract()
        self.transform()
        self.load()
        print("Pipeline completed")

