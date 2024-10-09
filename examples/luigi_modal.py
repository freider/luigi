import enum
import traceback
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
        print("Checking completeness of", task_id)
        is_complete = task.complete()
        result_queue.put((op, (task.task_id, is_complete)))
        print("Completed checking completeness of", task_id)
    elif op == LuigiMethod.run:
        try:
            print("Running task", task_id)
            task.run()
        except Exception as e:
            print("Error running task", task_id)
            result_queue.put((op, (task_id, FAILED, str(e), [])))
            raise
        else:
            print("Task complete", task_id)
            result_queue.put((op, (task_id, DONE, "", [])))
