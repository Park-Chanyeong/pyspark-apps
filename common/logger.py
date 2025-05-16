import logging


class Logger:
    def __init__(self, app_name):
        logging.basicConfig(
            level=logging.INFO,
            format=f'[{app_name}][%(levelname)s][%(asctime)s.%(msecs)03d]%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(logging.DEBUG)
        self.app_name = app_name
        self.step_num = 0
        self.last_epoch_id = 0

    def write_log(self, log_type: str, msg: str, epoch_id: int=None):
        log_type_lst = ['debug', 'info', 'warning', 'error', 'critical']
        if log_type.lower() not in log_type_lst: return

        # 동일 epoch_id 에서는 self.step_num 값이 증가하도록 하고 새로운 epoch_id 에서 1로 초기화합니다.
        if self.last_epoch_id == epoch_id:
            self.step_num += 1
        else:
            self.step_num = 1
            self.last_epoch_id = epoch_id

        if epoch_id is None:
            getattr(self.logger, log_type.lower())(f'[STEP{self.step_num}] {msg}')
        else:
            getattr(self.logger, log_type.lower())(f'[EPOCH:{epoch_id}][STEP{self.step_num}] {msg}')