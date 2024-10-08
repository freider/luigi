import time

import modal
import luigi

def env(*args, **kwargs):
    def decorator(fun):
        print("setting env")
        fun.modal_env = args, kwargs
        return fun
    return decorator


class RootTask(luigi.Task):
    @env(gpu="A10G")
    def run(self):
        print("Running parent")
    
    def requires(self):
        return [ChildTask(i=i) for i in range(10)]


class ChildTask(luigi.Task):
    i = luigi.IntParameter()

    @env(gpu="A10G", image=modal.Image.debian_slim().pip_install("pandas"))
    def run(self):
        print("Running on Modal", self.i)
        import pandas as pd
        print(pd.DataFrame({"a": [1, 2, 3]}))
        time.sleep(1)


if __name__ == "__main__":
    luigi.build([ChildTask(1)], local_scheduler=True)
