from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule


INPUT_DIR = Path("/opt/airflow/data/input")
OUTPUT_DIR = Path("/opt/airflow/data/output")
REPORT_DIR = Path("/opt/airflow/reports")
ALERT_DIR = Path("/opt/airflow/alerts")


def ghi_log_retry(context):
    """
    Ham nay duoc goi khi task bi loi va Airflow chuan bi retry.
    """
    ALERT_DIR.mkdir(parents=True, exist_ok=True)

    task_instance = context.get("task_instance")
    ten_task = task_instance.task_id
    lan_thu = task_instance.try_number

    file_log = ALERT_DIR / "retry_log.txt"

    with file_log.open("a", encoding="utf-8") as f:
        f.write("\n=== TASK RETRY ===\n")
        f.write(f"Thoi gian: {datetime.now()}\n")
        f.write(f"Task: {ten_task}\n")
        f.write(f"Lan thu: {lan_thu}\n")


def ghi_log_loi(context):
    """
    Ham nay duoc goi khi task that bai sau khi da retry.
    """
    ALERT_DIR.mkdir(parents=True, exist_ok=True)

    task_instance = context.get("task_instance")
    ten_task = task_instance.task_id
    loi = context.get("exception")

    file_log = ALERT_DIR / "failure_log.txt"

    with file_log.open("a", encoding="utf-8") as f:
        f.write("\n=== TASK THAT BAI ===\n")
        f.write(f"Thoi gian: {datetime.now()}\n")
        f.write(f"Task: {ten_task}\n")
        f.write(f"Loi: {loi}\n")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "on_retry_callback": ghi_log_retry,
    "on_failure_callback": ghi_log_loi,
}


@dag(
    dag_id="giam_sat_loi_retry",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["nhom6", "udpt", "retry"],
)
def giam_sat_loi_retry():

    @task
    def lay_danh_sach_file():
        """
        Lay danh sach file txt trong thu muc input.
        """
        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        ALERT_DIR.mkdir(parents=True, exist_ok=True)

        ds_file = []

        for file in INPUT_DIR.glob("*.txt"):
            ds_file.append(str(file))

        print("Danh sach file:")
        print(ds_file)

        return ds_file

    @task
    def kiem_tra_va_xu_ly_file(duong_dan_file):
        """
        Xu ly file.
        Neu file co chu ERROR thi cho task bi loi de demo retry.
        """
        file_path = Path(duong_dan_file)
        noi_dung = file_path.read_text(encoding="utf-8")

        if "ERROR" in noi_dung:
            raise AirflowException(
                f"File {file_path.name} co noi dung ERROR nen task bi loi"
            )

        so_tu = len(noi_dung.split())

        file_ket_qua = OUTPUT_DIR / f"{file_path.stem}_retry_ket_qua.txt"

        file_ket_qua.write_text(
            f"Ten file: {file_path.name}\n"
            f"So tu: {so_tu}\n"
            f"Trang thai: THANH CONG\n",
            encoding="utf-8",
        )

        print(f"Xu ly thanh cong file: {file_path.name}")

        return {
            "ten_file": file_path.name,
            "trang_thai": "THANH CONG",
            "so_tu": so_tu,
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def tao_bao_cao_giam_sat():
        """
        Tao bao cao giam sat loi.
        Task nay van chay ke ca khi co task xu ly file bi loi.
        """
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        ALERT_DIR.mkdir(parents=True, exist_ok=True)

        file_retry = ALERT_DIR / "retry_log.txt"
        file_failure = ALERT_DIR / "failure_log.txt"

        noi_dung_retry = "Khong co task nao retry."
        noi_dung_failure = "Khong co task nao that bai."

        if file_retry.exists():
            noi_dung_retry = file_retry.read_text(encoding="utf-8")

        if file_failure.exists():
            noi_dung_failure = file_failure.read_text(encoding="utf-8")

        bao_cao = []
        bao_cao.append("BAO CAO GIAM SAT LOI VA RETRY")
        bao_cao.append("==============================")
        bao_cao.append(f"Thoi gian tao bao cao: {datetime.now()}")
        bao_cao.append("")
        bao_cao.append("1. Thong tin retry:")
        bao_cao.append(noi_dung_retry)
        bao_cao.append("")
        bao_cao.append("2. Thong tin task that bai:")
        bao_cao.append(noi_dung_failure)
        bao_cao.append("")
        bao_cao.append("Ket luan:")
        bao_cao.append(
            "Workflow da co co che retry va ghi log khi task gap loi."
        )

        file_bao_cao = REPORT_DIR / "bao_cao_giam_sat_loi_retry.txt"
        file_bao_cao.write_text("\n".join(bao_cao), encoding="utf-8")

        print("Da tao bao cao giam sat loi")

        return str(file_bao_cao)

    ds_file = lay_danh_sach_file()

    ket_qua = kiem_tra_va_xu_ly_file.expand(duong_dan_file=ds_file)

    ket_qua >> tao_bao_cao_giam_sat()


giam_sat_loi_retry()