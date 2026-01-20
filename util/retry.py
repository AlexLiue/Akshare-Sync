from util.logger import get_logger


def log_retry_stats(retry_state):
    logger = get_logger("retry", "data-sync")
    fn_name = retry_state.fn.__name__
    attempt = retry_state.attempt_number
    sleep = retry_state.next_action.sleep
    last_exc = retry_state.outcome.exception()
    args = retry_state.args
    kwargs = retry_state.kwargs
    logger.warning(
        f"Exec Function [{fn_name}] Failed, Retry [{attempt}] After {int(sleep)} Seconds, Caused By [{last_exc}], args[{args}] kwargs[{kwargs}] "
    )

