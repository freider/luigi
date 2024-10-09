import enum
import modal
import luigi
from luigi.task_status import DONE, FAILED


def env(*args, **kwargs):
    def decorator(fun):
        fun.modal_env = args, kwargs
        return fun
    return decorator


class LuigiMethod(enum.Enum):
    check_complete = "check_complete"
    run = "run"

def task_runner(task: luigi.Task, task_id: str, op: LuigiMethod, result_queue: modal.Queue):
    if op == LuigiMethod.check_complete:
        is_complete = task.complete()
        result_queue.put((task.task_id, is_complete))

    elif op == LuigiMethod.run:
        try:
            task.run()
        except Exception as e:
            return result_queue.put((task_id, FAILED, str(e), []))
        else:
            return result_queue.put((task_id, DONE, "", []))
