import luigi


def env(*args, **kwargs):
    def decorator(fun):
        fun.modal_env = args, kwargs
        return fun
    return decorator



def task_runner(task: luigi.Task):
    task.run()
