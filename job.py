from sparkTools.sparkJob import SparkJob
import pandas as pd


class Job(SparkJob):
    def map_func(self):
        a = {
            'a': [1, 2, 3],
            'b': [1, 2, 3],
            'c': [1, 2, 3],
            'd': [1, 2, 3],
        }
        df = pd.DataFrame.from_dict(a, orient='index').T
        return df

    def reduce_func(self, data):
        df = data[['a', 'd']]
        return df


if __name__ == '__main__':
    job = Job()
    result = job.run()
    print("result: ", result)
