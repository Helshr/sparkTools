import abc
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext


class SparkJob:
    def _get_session(self):
        sc = self._get_sc()
        session = SparkSession(sc)
        return session

    def _get_sc(self):
        sc = SparkContext.getOrCreate()
        return sc

    @abc.abstractmethod
    def map_func(self):
        """ collect will reduce data(only suppose DataFrame data type)"""
        return

    @abc.abstractmethod
    def reduce_func(self, data):
        """ reduce data by this func """
        return

    def _reduce(self, data):
        d = data.asDict()
        df = pd.DataFrame.from_dict(d, orient='index').T
        return self.reduce_func(df)

    def run(self):
        session = self._get_session()
        raw_data = self.map_func()
        jdf = session.createDataFrame(raw_data)
        refs = jdf.rdd.map(self._reduce).collect()
        session.sparkContext.stop()
        return refs
