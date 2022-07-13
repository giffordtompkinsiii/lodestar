from lodestar import logger
from abc import ABC
from typing import List, Dict
from lodestar.database import models
from lodestar.database.maps import asset_map


class Pipeline(ABC):
    transformed_data = None
    extracted_data = None
    session = models.session
    logger = logger
    LandingClass = None

    def __init__(self, symbol: str, debug: bool = False):
        self.symbol = symbol
        self.debug = debug
        self.__name__ = f"{symbol} {__class__.__name__}"

    @classmethod
    def extract(cls) -> Dict:
        pass

    @classmethod
    def transform(cls) -> List[Dict]:
        pass

    @classmethod
    def load(cls) -> None:
        data = cls.transformed_data if cls.transformed_data else cls.transform()
        records = [cls.LandingClass(**d) for d in data]
        cls.session.add_all(records)
        cls.session.commit()

    # @classmethod
    def run_pipeline(self):
        self.extract()
        self.transform()
        self.load()
        print(f"{self.__name__} completed")


def bulk_pipeline(pipeline: Pipeline):
    for a in asset_map.values():
        pipeline(a.symbol, False).run_pipeline()
