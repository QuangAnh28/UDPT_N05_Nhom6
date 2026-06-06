from datetime import datetime
from pathlib import Path
from airflow.decorators import dag, task
INPUT_DIR = Path("/opt/airflow/data/input")
OUTPUT_DIR = Path("/opt/airflow/data/output")
REPORT_DIR = Path("/opt/airflow/reports")
@dag(
    dag_id="xu_ly_file_song_song",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nhom6", "udpt"],
)
def xu_ly_file_song_song():
    @task
    def lay_danh_sach_file():
        """
        Task 1: Lay danh sach cac file txt trong thu muc input.
        """
        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        ds_file = []
        for file in INPUT_DIR.glob("*.txt"):
            ds_file.append(str(file))
        print("Danh sach file tim duoc:")
        print(ds_file)
        return ds_file
    @task
    def xu_ly_1_file(duong_dan_file):
        """
        Task 2: Xu ly tung file.
        Moi file se duoc Airflow tao thanh mot task rieng.
        """
        file_path = Path(duong_dan_file)
        noi_dung = file_path.read_text(encoding="utf-8")
        so_dong = len(noi_dung.splitlines())
        so_tu = len(noi_dung.split())
        so_ky_tu = len(noi_dung)
        ket_qua = {
            "ten_file": file_path.name,
            "so_dong": so_dong,
            "so_tu": so_tu,
            "so_ky_tu": so_ky_tu,
        }
        file_ket_qua = OUTPUT_DIR / f"{file_path.stem}_ket_qua.txt"
        file_ket_qua.write_text(
            f"Ten file: {file_path.name}\n"
            f"So dong: {so_dong}\n"
            f"So tu: {so_tu}\n"
            f"So ky tu: {so_ky_tu}\n",
            encoding="utf-8",
        )
        print(f"Da xu ly xong file: {file_path.name}")
        return ket_qua
    @task
    def tong_hop_ket_qua(ds_ket_qua):
        """
        Task 3: Tong hop ket qua tu cac task xu ly file.
        """
        bao_cao = []
        bao_cao.append("BAO CAO XU LY FILE SONG SONG")
        bao_cao.append("============================")
        bao_cao.append(f"Tong so file: {len(ds_ket_qua)}")
        bao_cao.append("")
        tong_so_tu = 0
        tong_so_ky_tu = 0
        for item in ds_ket_qua:
            bao_cao.append(
                f"{item['ten_file']}: "
                f"{item['so_dong']} dong, "
                f"{item['so_tu']} tu, "
                f"{item['so_ky_tu']} ky tu"
            )
            tong_so_tu += item["so_tu"]
            tong_so_ky_tu += item["so_ky_tu"]
        bao_cao.append("")
        bao_cao.append(f"Tong so tu cua tat ca file: {tong_so_tu}")
        bao_cao.append(f"Tong so ky tu cua tat ca file: {tong_so_ky_tu}")
        file_bao_cao = REPORT_DIR / "bao_cao_xu_ly_file_song_song.txt"
        file_bao_cao.write_text("\n".join(bao_cao), encoding="utf-8")
        print("Da tao bao cao tong hop")
        return str(file_bao_cao)
    ds_file = lay_danh_sach_file()
    ds_ket_qua = xu_ly_1_file.expand(duong_dan_file=ds_file)
    tong_hop_ket_qua(ds_ket_qua)
xu_ly_file_song_song()