import os
from datetime import datetime

import pytest
import requests
import responses

from dwdown.download import ForecastDownloader

FIXTURE_HTML_PATH = "fixtures/relhum_index.html"
os.makedirs(os.path.dirname(FIXTURE_HTML_PATH), exist_ok=True)

TEST_URL = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/03/relhum/"


@pytest.fixture
def html_content():
    html = requests.get(TEST_URL).text
    with open("fixtures/relhum_index.html", "w", encoding="utf-8") as f:
        f.write(html)

    with open(FIXTURE_HTML_PATH, encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def downloader(tmp_path):
    return ForecastDownloader(
        model="icon-d2",
        forecast_run="03",
        variable="relhum",
        grid=None,
        files_path=str(tmp_path / "downloads"),
        log_files_path=str(tmp_path / "logs"),
        delay=1,
        n_jobs=1,
        retry=0,
        timeout=30,
        url=TEST_URL,
        base_url=None,
        xpath_files=None,
        xpath_meta_data=None)


@responses.activate
def test_get_data_dates(html_content, downloader):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)

    min_date, max_date = downloader.get_data_dates(
        url=None,
        date_pattern=None,)

    assert isinstance(min_date, datetime)
    assert isinstance(max_date, datetime)
    assert min_date <= max_date


@responses.activate
def test_get_links_filters_correctly(html_content, downloader):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)

    date = datetime.now()
    formatted_date = date.strftime("%Y%m%d") + '03'

    links = downloader.get_links(
        prefix="icon-d2_germany",
        suffix=".grib2.bz2",
        exclude_pattern=["_000_", "_1000_"],
        include_pattern=["relhum", formatted_date],
        min_timestep=0,
        max_timestep=20)

    assert len(links) != 0
    assert all(link.endswith(".grib2.bz2") for link in links)
    assert all("_000_" not in link for link in links)
    assert all("_1000_" not in link for link in links)
    assert all("_030_" not in link for link in links)
    assert all("relhum" in link for link in links)
    assert all(formatted_date in link for link in links)


@responses.activate
def test_get_links_no_filtering(html_content, downloader):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)
    links = downloader.get_links(
        prefix=None,
        suffix=None,
        include_pattern=None,
        exclude_pattern=None,
        additional_patterns=None,
        skip_time_step_filtering_variables=None,
        min_timestep=None,
        max_timestep=None,)

    assert len(links) == 1078


@responses.activate
@pytest.mark.parametrize("prefix,expected_count", [
    ("icon-d2", 1078),
    ("", 1078),
    (None, 1078),
    ("nonsense", 0),
])
def test_get_links_prefix_filtering(html_content, downloader, prefix, expected_count):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)
    links = downloader.get_links(prefix=prefix)
    assert len(links) == expected_count


@responses.activate
@pytest.mark.parametrize("suffix,expected_count", [
    (".grib2.bz2", 1078),
    ("", 1078),
    (None, 1078),
    ("nonsense", 0),
])
def test_get_links_prefix_filtering(html_content, downloader, suffix, expected_count):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)
    links = downloader.get_links(suffix=suffix)
    assert len(links) == expected_count


@responses.activate
@pytest.mark.parametrize("exclude_pattern,expected_count", [
    (["icosahedral"], 539),
    (["regular"], 539),
])
def test_get_links_exclude_filtering(html_content, downloader, exclude_pattern, expected_count):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)
    links = downloader.get_links(exclude_pattern=exclude_pattern)
    assert len(links) == expected_count


@pytest.mark.parametrize("additional_patterns,expected_count", [
    ({"relhum": []}, 0),
    ({"relhum": [1000]}, 98),
    ({"relhum": [1000, 200]}, 196),
])
@responses.activate
def test_get_links_with_additional_patterns_parametrized(html_content, downloader, additional_patterns, expected_count):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)

    links = downloader.get_links(additional_patterns=additional_patterns)
    assert len(links) == expected_count, (
        f"Expected {expected_count} links for additional_patterns={additional_patterns}, got {len(links)}")


@pytest.mark.parametrize("min_ts,max_ts,expected_count", [
    (0, 0, 22),       # Only timestep 0
    (0, 10, 242),     # Range 0–10 inclusive
    (None, None, 1078)  # No timestep filtering
])
@responses.activate
def test_get_links_with_timestep_range_parametrized(html_content, downloader, min_ts, max_ts, expected_count):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)

    links = downloader.get_links(
        min_timestep=min_ts,
        max_timestep=max_ts,)

    assert len(links) == expected_count, f"Expected {expected_count} links for min_ts={min_ts}, max_ts={max_ts}, got {len(links)}"


@pytest.mark.parametrize("min_timestep,max_timestep,exclude_pattern,expected_count", [
    (0, 10, ["regular"], 121),
    (0, 5, ["regular"], 66),
])
@responses.activate
def test_get_links_with_timestep_and_exclude_combined_parametrized(
    html_content, downloader, min_timestep, max_timestep, exclude_pattern, expected_count
):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)

    links = downloader.get_links(
        min_timestep=min_timestep,
        max_timestep=max_timestep,
        exclude_pattern=exclude_pattern,)

    assert len(links) == expected_count, (
        f"Expected {expected_count} links for min={min_timestep}, max={max_timestep}, "
        f"exclude={exclude_pattern}, got {len(links)}")


@pytest.mark.parametrize("min_timestep,max_timestep,exclude_pattern,additional_patterns,expected_count", [
    (0, 5, ["regular"], {"relhum": []}, 0),
    (0, 5, ["regular"], {"relhum": [1000]}, 6),
])
@responses.activate
def test_get_links_with_timestep_exclude_and_additional_patterns_combined(
    html_content, downloader, min_timestep, max_timestep, exclude_pattern, additional_patterns, expected_count
):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)

    links = downloader.get_links(
        min_timestep=min_timestep,
        max_timestep=max_timestep,
        exclude_pattern=exclude_pattern,
        additional_patterns=additional_patterns,)

    assert len(links) == expected_count, (
        f"Expected {expected_count} links for timestep=({min_timestep}, {max_timestep}), "
        f"exclude={exclude_pattern}, additional={additional_patterns}, got {len(links)}")


@responses.activate
def test_get_links_with_invalid_url(downloader):
    responses.add(responses.GET, TEST_URL, status=404)

    links = downloader.get_links()
    assert links == []


#@responses.activate
def test_download_success():
    downloader = ForecastDownloader(
        model="icon-d2",
        forecast_run="03",
        variable="relhum",
        grid=None,
        files_path="downloads",
        log_files_path="logs",
        delay=0.1,
        n_jobs=10,
        retry=0,
        timeout=30,
        # url=TEST_URL,
        base_url=None,
        xpath_files=None,
        xpath_meta_data=None)

    date = datetime.now()
    formatted_date = date.strftime("%Y%m%d") + '03'

    links = downloader.get_links(
        prefix="icon-d2_germany",
        suffix=".grib2.bz2",
        exclude_pattern=["_000_", "_1000_"],
        include_pattern=["relhum", formatted_date],
        min_timestep=0,
        max_timestep=1)

    downloader.download(check_for_existence=False)

    for link in links:
        fname = os.path.basename(link)
        target_file = os.path.join("downloads", "03", "relhum", fname)
        assert os.path.exists(target_file)

    assert len(downloader.downloaded_files) == len(links)
    assert len(downloader.failed_files) == 0

    downloader.delete()
    assert not os.path.exists("downloads/03")


def test_delete(tmp_path, downloader):
    # Create dummy downloaded files
    dpath = tmp_path / "downloads" / "03" / "relhum"
    dpath.mkdir(parents=True)
    test_file = dpath / "test.grib2.bz2"
    test_file.write_bytes(b"123")
    downloader._downloaded_files_paths = [str(test_file)]

    assert test_file.exists()
    downloader.delete()
    assert not test_file.exists()
