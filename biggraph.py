import time

import modal
import luigi
from luigi_modal import env

luigi_image = modal.Image.debian_slim().pip_install("luigi")

@env(image=luigi_image)
class RootTask(luigi.Task):    
    def run(self):
        print("Running parent")
    
    def requires(self):
        return [ChildTask(i=i) for i in range(10)]

@env(gpu="A10G", image=luigi_image.pip_install("pandas"))
class ChildTask(luigi.Task):
    i = luigi.IntParameter()

    def run(self):
        print("Running on Modal", self.i)
        time.sleep(1)


if __name__ == "__main__":
    luigi.build([RootTask()], local_scheduler=True)
