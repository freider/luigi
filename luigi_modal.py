import modal
import luigi
from luigi.task_status import DONE, FAILED


def env(*args, **kwargs):
    def decorator(fun):
        fun.modal_env = args, kwargs
        return fun
    return decorator



def task_runner(task: luigi.Task, task_id: str, result_queue: modal.Queue):
    try:
        task.run()
    except Exception as e:
        return result_queue.put((task_id, FAILED, str(e), []))
    else:
        return result_queue.put((task_id, DONE, "", []))
