import logging
import sys
import time
from datetime import date

# from datetime import datetime as dt
from datetime import timedelta as td
from pathlib import Path

import requests
from dateutil.relativedelta import relativedelta

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

START_DATE = date(2016, 4, 12)  # Default start date
END_DATE = date(2016, 5, 13)  # Default end date


def create_dir(path_list: list[str]) -> Path:
    """Create directories specified in path_list."""
    current_dir = Path()
    for directory in path_list:
        current_dir /= directory
        current_dir.mkdir(parents=True, exist_ok=True)
    return current_dir


class AmedasNode:
    """Represents an AMeDAS observation station."""

    def __init__(
        self,
        prec_no: str,
        block_no: str,
        name: str,
        _id: str,
        area_code: str,
        group_code: str,
        height: str,
    ):
        self.block_no = block_no
        self.prec_no = prec_no
        self.name = name
        self.id = _id
        self.area_code = area_code
        self.group_code = group_code
        self.height = float(height)
        self.url_part = "a" if int(block_no) < 10000 else "s"

    def get_data(
        self, data_type: str = "10min", target_date: date | None = None
    ) -> str | None:
        """Download specified type of observation data."""
        url = self._construct_url(data_type, target_date)
        logger.info("--target url-- %s", url)
        return self._download_html(url)

    def _construct_url(self, data_type: str, target_date: date) -> str | None:
        """Construct the download URL."""
        if data_type in ["10min", "hourly", "daily"]:
            return (
                f"http://www.data.jma.go.jp/obd/stats/etrn/view/{data_type}_{self.url_part}1.php?"
                f"prec_no={self.prec_no}&block_no={self.block_no}&year={target_date.year}&"
                f"month={target_date.month:02}&day={target_date.day:02}&view="
            )
        if data_type == "real-time":
            return f"http://www.jma.go.jp/jp/amedas_h/today-{self.id}.html"
        logger.warning("Unknown data type: %s", data_type)
        return None

    def _download_html(self, url: str) -> str | None:
        """Download HTML from the specified URL."""
        try:
            response = requests.get(url)
            response.encoding = "utf-8"  # Correct encoding
            return response.text
        except Exception as e:
            logger.error("--download error-- %s", str(e))
            return None

    def save(
        self,
        data_type: str,
        target_date: date,
        *,
        force: bool = False,
    ) -> bool:
        """Download and save observation data."""
        date_to_use = (
            date(year=target_date.year, month=target_date.month, day=1)
            if data_type == "daily"
            else target_date
        )
        dir_path = create_dir(
            [
                "raw_html",
                data_type,
                f"{self.block_no}_{self.name}",
                str(date_to_use.year),
            ]
        )
        file_name = f"{self.block_no}_{self.name}_{date_to_use:%Y_%m_%d}.html"
        file_path = dir_path / file_name

        if not file_path.exists() or force:
            html = self.get_data(data_type, date_to_use)
            if html:
                with file_path.open("w", encoding="utf-8-sig") as fw:
                    fw.write(html)
                logger.info("Data saved to %s", file_path)
            return True
        logger.info("%s already exists.", file_path)
        return False


def get_amedas_nodes() -> dict[str, AmedasNode]:
    """Return a dictionary of AMeDAS nodes."""
    amedas_nodes = {}
    fname = Path(__file__).parent / "AMeDAS_list.csv"

    with fname.open("r", encoding="utf-8-sig") as fr:
        for line in fr:
            fields = line.strip().split("\t")
            fields = [None if x == "None" else x for x in fields]
            if len(fields) < 10:
                continue  # Skip invalid lines
            prec_no, block_no, name, *_, height, _id, area_code, group_code = fields
            amedas_nodes[block_no] = AmedasNode(
                prec_no, block_no, name, _id, area_code, group_code, height
            )

    return amedas_nodes


def main() -> None:
    """Execute the download process."""
    # data_type = sys.argv[1]
    force_download = "-f" in sys.argv
    amedas_nodes = get_amedas_nodes()
    # print(amedas_nodes)

    # read from target.yaml
    import yaml

    with Path("target.yaml").open("r") as f:
        target = yaml.safe_load(f)
    start_date = target["start_date"]
    end_date = target["end_date"]
    resolutions = target["resolutions"]
    target_nodes = target["targets"]

    # Ensure that naive datetime is acceptable for your application
    # if data_type == "real-time":
    # start_date = end_date = dt.now()

    for resolution in resolutions:
        date_iter = start_date
        while date_iter <= end_date:
            for block_no in target_nodes:
                if block_no in amedas_nodes:
                    node = amedas_nodes[block_no]
                    node.save(resolution, date_iter, force=force_download)
                    time.sleep(0.2)  # Respectful download interval
                else:
                    logger.warning("Unknown block number: %s", block_no)
            date_iter += (
                td(days=1)
                if resolution in ["10min", "hourly", "real-time"]
                else relativedelta(months=1)
            )


if __name__ == "__main__":
    main()
