from abc import ABC, abstractmethod

from .. import Pipeline


class BulkPipeline(Pipeline, ABC):

    def __init__(self, pipeline: Pipeline, pipeline_objects):
        self.pipeline = pipeline
        self.pipeline_objects = pipeline_objects

    # @abstractmethod
    def run_bulk_pipeline(self):
        for o in self.pipeline_objects:
            p = self.pipeline(o)
            p.run_pipeline()
