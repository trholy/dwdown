import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses

from dwdown.download import ForecastDownloader

FIXTURE_HTML_PATH = "fixtures/relhum_index.html"
os.makedirs(os.path.dirname(FIXTURE_HTML_PATH), exist_ok=True)

TEST_URL = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/03/relhum/"


@pytest.fixture
def html_content():
    # Attempt to read fixture or fetch if missing (for reproduction/completeness)
    if os.path.exists(FIXTURE_HTML_PATH):
        with open(FIXTURE_HTML_PATH, encoding="utf-8") as f:
            return f.read()
    
    # Fallback if fixture missing (should not happen in proper test env but helps dev)
    try:
        html = requests.get(TEST_URL).text
        with open(FIXTURE_HTML_PATH, "w", encoding="utf-8") as f:
            f.write(html)
        return html
    except:
        return "" # Should be handled by mock response if needed


@pytest.fixture
def mock_handlers():
    """Mock the handlers used by ForecastDownloader."""
    with patch("dwdown.download.forecast_download.SessionHandler") as MockSessionHandler, \
         patch("dwdown.download.forecast_download.LogHandler") as MockLogHandler, \
         patch("dwdown.download.forecast_download.FileHandler") as MockFileHandler, \
         patch("dwdown.download.forecast_download.DateHandler") as MockDateHandler, \
         patch("dwdown.download.forecast_download.TimeHandler") as MockTimeHandler:
        
        # Setup SessionHandler to return a REAL session so 'responses' works
        session_instance = MockSessionHandler.return_value
        session_instance._session = requests.Session()
        session_instance.get_session.return_value = session_instance._session
        
        # Setup other mocks
        file_instance = MockFileHandler.return_value

        yield {
            "session": MockSessionHandler,
            "log": MockLogHandler,
            "file": MockFileHandler,
            "date": MockDateHandler,
            "time": MockTimeHandler,
            "file_instance": file_instance
        }


@pytest.fixture
def downloader(tmp_path, mock_handlers):
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

    downloader._datehandler._parse_dates.return_value = [
        datetime(2023, 1, 1), datetime(2023, 1, 2)
    ]

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
    
    # Mock simple filter to return specific list
    expected_links = ["link1.grib2.bz2"]
    downloader._filehandler._simple_filename_filter.return_value = expected_links
    
    # Mock advanced filter to return pass-through or processed list
    full_urls = [requests.compat.urljoin(TEST_URL, l) for l in expected_links]
    downloader._filehandler._advanced_filename_filter.return_value = full_urls
    
    links = downloader.get_links(
        prefix="icon-d2_germany",
        suffix=".grib2.bz2",
        exclude_pattern=["_000_", "_1000_"],
        include_pattern=["relhum", formatted_date],
        min_timestep=0,
        max_timestep=20)
    
    # get_links returns the result of advanced filter
    assert links == full_urls
    downloader._filehandler._simple_filename_filter.assert_called()


@responses.activate
def test_get_links_no_filtering(html_content, downloader):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)
    
    # Simulate filter returning input list, but filtering out parent directory (links ending in /)
    # This ensures we count only files, matching the expected 1078 count.
    downloader._filehandler._simple_filename_filter.side_effect = lambda filenames, **kwargs: [f for f in filenames if not f.endswith('/')]
    
    # Mock advanced filter to return input (urls)
    downloader._filehandler._advanced_filename_filter.side_effect = lambda filenames, **kwargs: filenames
    
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


# Simplified parametrization tests to check delegation
@responses.activate
def test_get_links_delegation(html_content, downloader):
    responses.add(responses.GET, TEST_URL, body=html_content, status=200)
    
    downloader._filehandler._simple_filename_filter.return_value = []
    # advanced filter returns empty list by default mock (MagicMock is iterable? No, need to set it)
    downloader._filehandler._advanced_filename_filter.return_value = []
    
    downloader.get_links(prefix="test")
    
    downloader._filehandler._simple_filename_filter.assert_called()


@responses.activate
def test_get_links_with_invalid_url(downloader):
    responses.add(responses.GET, TEST_URL, status=404)

    links = downloader.get_links()
    assert links == []


def test_download_success(downloader, tmp_path):
    mock_links = ["http://example.com/file1.grib2"]
    downloader.get_links = MagicMock(return_value=mock_links)
    
    # Needs to set download_links as get_links is mocked and not calling implementation
    downloader.download_links = mock_links
    
    # Ensure directory exists because filehandler._ensure_directory_exists is mocked and won't create it.
    # ForecastDownloader uses: os.path.join(files_path, forecast_run, variable)
    # files_path is from downloader fixture -> tmp_path / "downloads"
    # forecast_run="03", variable="relhum"
    download_dir = tmp_path / "downloads" / "03" / "relhum"
    download_dir.mkdir(parents=True, exist_ok=True)

    # Mock session get for the file download
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, "http://example.com/file1.grib2", body=b"DATA", status=200)
        downloader.download(check_for_existence=False)

    # Verify download logic (internal details: downloaded_files list updated)
    assert len(downloader.downloaded_files) == 1
    assert downloader.downloaded_files[0] == "http://example.com/file1.grib2"
    assert len(downloader.failed_files) == 0


def test_delete(downloader):
    downloader._downloaded_files_paths = ["file1", "file2"]
    downloader.delete()
    
    downloader._filehandler._delete_files_safely.assert_called_with(["file1", "file2"], "downloaded file")


def test_get_variable_from_link():
    from dwdown.download.forecast_download import ForecastDownloader
    
    link = "https://example.com/data/temperature/file.grib"
    result = ForecastDownloader._get_variable_from_link(link)
    assert result == "temperature"


def test_get_variable_from_link_short_path():
    from dwdown.download.forecast_download import ForecastDownloader
    
    link = "https://example.com/file.grib"
    result = ForecastDownloader._get_variable_from_link(link)
    assert result == ""


def test_set_grid_filter_icosahedral():
    from dwdown.download.forecast_download import ForecastDownloader
    
    result = ForecastDownloader._set_grid_filter("icosahedral")
    assert result == ["icosahedral"]


def test_set_grid_filter_regular():
    from dwdown.download.forecast_download import ForecastDownloader
    
    result = ForecastDownloader._set_grid_filter("regular")
    assert result == ["regular"]


def test_set_grid_filter_none():
    from dwdown.download.forecast_download import ForecastDownloader
    
    result = ForecastDownloader._set_grid_filter(None)
    assert result == []


def test_set_grid_filter_invalid():
    import pytest

    from dwdown.download.forecast_download import ForecastDownloader
    
    with pytest.raises(ValueError):
        ForecastDownloader._set_grid_filter("invalid")


@responses.activate
def test_get_filenames_from_url(downloader):
    from lxml import html
    sample_html = '<html><body><a href="file1.bz2">File1</a><a href="file2.bz2">File2</a></body></html>'
    responses.add(responses.GET, TEST_URL, body=sample_html, status=200)
    
    downloader._xpath_files = './/a/@href'
    
    result = downloader._get_filenames_from_url()
    
    assert 'file1.bz2' in result
    assert 'file2.bz2' in result
