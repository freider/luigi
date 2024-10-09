import random
import time

import luigi
import modal
from luigi_modal import env

luigi_image = modal.Image.debian_slim().pip_install("luigi")

GENERATE_FEATURES_CONCURRENCY = 50
COLLECT_DATA_MULTIPLIER = 2
DEPLOY_CONCURRENCY = 10


def random_sleep():
    # 20% 0-5s
    # 70% 5-10s
    # 10% 10-15s
    rand = random.uniform(0, 1)
    if rand < 0.20:
        sleep_time = random.uniform(0, 5)
    elif rand < 0.90:
        sleep_time = random.uniform(5, 10)
    else:
        sleep_time = random.uniform(10, 15)
    time.sleep(sleep_time)


@env(image=luigi_image)
class CollectData(luigi.Task):
    i = luigi.IntParameter()

    def run(self):
        random_sleep()


@env(image=luigi_image)
class GenerateFeatures(luigi.Task):
    i = luigi.IntParameter()

    def run(self):
        random_sleep()

    def requires(self):
        return [
            CollectData(i=i) for i in range(
                self.i * COLLECT_DATA_MULTIPLIER,
                self.i * COLLECT_DATA_MULTIPLIER + COLLECT_DATA_MULTIPLIER
            )
        ]


@env(gpu="A10G", image=luigi_image.pip_install("pandas"))
class TrainModel(luigi.Task):
    i = luigi.IntParameter()

    def run(self):
        random_sleep()

    def requires(self):
        return [GenerateFeatures(i=i) for i in range(GENERATE_FEATURES_CONCURRENCY)]


@env(image=luigi_image)
class TuneModel(luigi.Task):
    i = luigi.IntParameter()

    def run(self):
        random_sleep()

    def requires(self):
        return TrainModel(1)


@env(image=luigi_image)
class DeployModel(luigi.Task):
    def run(self):
        random_sleep()

    def requires(self):
        return [TuneModel(i=i) for i in range(DEPLOY_CONCURRENCY)]

    def complete(self):
        # simulated slow completeness check
        time.sleep(2)
        return False


if __name__ == "__main__":
    luigi.build([DeployModel()], local_scheduler=False)
