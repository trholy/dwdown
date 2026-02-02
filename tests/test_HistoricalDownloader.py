import zipfile
from unittest.mock import patch

import pandas as pd
import pytest
import requests
import responses

from dwdown.download import HistoricalDownloader

TEST_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/"


@pytest.fixture
def mock_handlers():
    """Mock the handlers used by HistoricalDownloader."""
    with patch("dwdown.download.historical_download.SessionHandler") as MockSessionHandler, \
         patch("dwdown.download.historical_download.LogHandler") as MockLogHandler, \
         patch("dwdown.download.historical_download.FileHandler") as MockFileHandler, \
         patch("dwdown.download.historical_download.TimeHandler") as MockTimeHandler:
        
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
            "time": MockTimeHandler,
            "file_instance": file_instance
        }


@pytest.fixture
def downloader(tmp_path, mock_handlers):
    """
    Creates a HistoricalDownloader instance with temporary paths.
    """
    downloads_dir = tmp_path / "downloads"
    extracted_dir = tmp_path / "extracted"
    logs_dir = tmp_path / "logs"
    
    # Ensure these exist as the real init might rely on mocked filehandler which won't create them
    downloads_dir.mkdir()
    extracted_dir.mkdir()
    logs_dir.mkdir()

    return HistoricalDownloader(
        base_url=TEST_URL,
        files_path=str(downloads_dir),
        extracted_files_path=str(extracted_dir),
        log_files_path=str(logs_dir),
        delay=0,  # No delay for tests
        retry=0
    )


@responses.activate
def test_get_filenames_from_url(downloader):
    sample_html = '''
    <html><body><pre>
    <a href="tageswerte_KL_00001_19370101_19860630_hist.zip">file1.zip</a>
    <a href="tageswerte_KL_00003_18910101_20110331_hist.zip">file2.zip</a>
    <a href="other_file.txt">other.txt</a>
    </pre></body></html>
    '''
    responses.add(responses.GET, TEST_URL, body=sample_html, status=200)
    
    filenames = downloader._get_filenames_from_url()
    
    assert "tageswerte_KL_00001_19370101_19860630_hist.zip" in filenames
    assert "tageswerte_KL_00003_18910101_20110331_hist.zip" in filenames
    assert "other_file.txt" in filenames


@responses.activate
def test_get_links_basic(downloader):
    sample_html = '<html><body><pre><a href="tageswerte_KL_00001_19370101_19860630_hist.zip">file1.zip</a></pre></body></html>'
    responses.add(responses.GET, TEST_URL, body=sample_html, status=200)

    # Mock filehandler simple_filename_filter to return the specific file
    # We mock it to simulate "no filtering" or "match all" behavior for this test
    downloader._filehandler._simple_filename_filter.return_value = ["tageswerte_KL_00001_19370101_19860630_hist.zip"]

    links = downloader.get_links(station_ids=['00001'])
    
    assert len(links) == 1
    assert links[0] == TEST_URL + "tageswerte_KL_00001_19370101_19860630_hist.zip"
    downloader._filehandler._simple_filename_filter.assert_called()


@responses.activate
def test_get_links_filter_station_id(downloader):
    sample_html = '<html><body><pre>' \
                  '<a href="tageswerte_KL_00001_19370101_19860630_hist.zip">Station 1</a>' \
                  '<a href="tageswerte_KL_00003_18910101_20110331_hist.zip">Station 3</a>' \
                  '</pre></body></html>'
    responses.add(responses.GET, TEST_URL, body=sample_html, status=200)
    
    # We expect the downloader logic to pad station IDs and add them to include_pattern
    # before calling the filehandler.
    
    downloader._filehandler._simple_filename_filter.return_value = []
    
    downloader.get_links(station_ids=["1", "3"])
    
    # Check arguments passed to _simple_filename_filter
    call_args = downloader._filehandler._simple_filename_filter.call_args
    assert call_args is not None
    kwargs = call_args.kwargs
    
    # Ensure include_pattern contains the user padded patterns
    assert "_00001_" in kwargs["include_pattern"]
    assert "_00003_" in kwargs["include_pattern"]


def test_download_success(downloader, tmp_path):
    mock_link = "http://example.com/data.zip"
    downloader.download_links = [mock_link]

    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, mock_link, body=b"ZIPDATA", status=200)
        
        downloader.download()
        
    assert mock_link in downloader.downloaded_files
    assert len(downloader.failed_files) == 0
    
    # Verify file content
    downloaded_file = tmp_path / "downloads" / "data.zip"
    assert downloaded_file.exists()
    assert downloaded_file.read_bytes() == b"ZIPDATA"


def test_download_retry_logic(downloader):
    mock_link = "http://example.com/fail_once.zip"
    downloader.download_links = [mock_link]
    downloader._retry = 2
    
    with responses.RequestsMock() as rsps:
        # First attempt fails with a ConnectionError (wrapped in RequestException by requests)
        rsps.add(responses.GET, mock_link, body=requests.exceptions.ConnectionError("Fail"))
        # Retry 1 succeeds
        rsps.add(responses.GET, mock_link, body=b"SUCCESS", status=200)
        
        downloader.download()
        
    assert mock_link in downloader.downloaded_files
    assert len(downloader.failed_files) == 0


def test_download_station_description(downloader, tmp_path):
    desc_filename = "KL_Tageswerte_Beschreibung_Stationen.txt"
    desc_url = TEST_URL + desc_filename
    
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, desc_url, body=b"Station Description Data", status=200)
        
        downloader.download_station_description()
        
    desc_file = tmp_path / "downloads" / desc_filename
    assert desc_file.exists()
    assert desc_file.read_bytes() == b"Station Description Data"


def test_read_station_description(downloader, tmp_path):
    # create dummy station description file in downloads folder
    desc_filename = "KL_Tageswerte_Beschreibung_Stationen.txt"
    desc_path = tmp_path / "downloads" / desc_filename
    
    # Define exact widths for columns matching the separator line style
    # id(5), von(8), bis(8), hoehe(13), lat(9), lon(9), name(40), land(20), ab(6)
    # But note: separators defined by dashes in DWD file usually match header or max data width.
    # We define widths for the dashes.
    widths = [11, 9, 9, 13, 9, 9, 39, 19, 6]
    headers = [
        "Stations_id", "von_datum", "bis_datum", "Stationshoehe", 
        "geoBreite", "geoLaenge", "Stationsname", "Bundesland", "Abgabe"
    ]
    
    # Construct lines
    # 1. Header Line
    header_line = " ".join(f"{h:<{w}}" for h, w in zip(headers, widths))
    
    # 2. Separator Line (dashes)
    separator_line = " ".join("-" * w for w in widths)
    
    # 3. Data Line 1
    # 00001: 5 chars. Pad to 11.
    val1 = ["00001", "19370101", "19860630", "478", "47.8413", "8.8493", "Aach", "Baden-Wuerttemberg", "Frei"]
    # Be careful with alignment inside the field. read_fwf strips whitespace by default.
    # Just ensure data fits in width.
    line1 = " ".join(f"{v:<{w}}" for v, w in zip(val1, widths))
    
    # 3. Data Line 2
    val2 = ["00003", "18910101", "20110331", "202", "50.7827", "6.0941", "Aachen", "Nordrhein-Westfalen", "Frei"]
    line2 = " ".join(f"{v:<{w}}" for v, w in zip(val2, widths))
    
    content = f"{header_line}\n{separator_line}\n{line1}\n{line2}\n"
    
    # Note: encoding default is windows-1252
    with open(desc_path, "w", encoding="windows-1252") as f:
        f.write(content)
        
    df = downloader.read_station_description()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.index.name == "Stations_id"
    # DWD station IDs are usually read as strings if we enforce it or if they have leading zeros preserved.
    # The refactor enforces dtype={"Stations_id": str}
    assert "00001" in df.index 
    assert "00003" in df.index
    
    # Check data content
    # strip() is important as fwf might leave trailing spaces depending on settings, though read_fwf usually strips.
    assert df.loc["00001", "Stationsname"].strip() == "Aach"
    assert df.loc["00001", "Bundesland"].strip() == "Baden-Wuerttemberg"


def test_unpack_zip(downloader, tmp_path):
    # Setup: Create a real zip file containing a couple of dummy text files
    zip_name = "tageswerte_KL_00001_19370101_19860630_hist.zip"
    zip_path = tmp_path / "downloads" / zip_name
    
    # Files inside zip
    data_filename = "produkt_klima_tag_19370101_19860630_00001.txt"
    meta_filename = "Metadaten_Geographie_00001.txt"
    
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr(data_filename, "Data content")
        zf.writestr(meta_filename, "Meta content")
        
    # Act: unpack
    # passing the list manually since download_links might be empty
    downloader.extract(zip_files=[zip_name])
    
    # Check
    extract_folder = tmp_path / "extracted" / "tageswerte_KL_00001_19370101_19860630_hist"
    assert extract_folder.exists()
    assert (extract_folder / data_filename).exists()
    assert (extract_folder / meta_filename).exists()
    assert (extract_folder / data_filename).read_text() == "Data content"


def test_unpack_zip_hist_only(downloader, tmp_path):
    # Setup: Create zip with noise files and one valid data file matching station id
    zip_name = "tageswerte_KL_00001_19370101_19860630_hist.zip"
    zip_path = tmp_path / "downloads" / zip_name
    
    valid_data = "produkt_klima_tag_19370101_19860630_00001.txt"
    other_data = "produkt_klima_tag_19370101_19860630_99999.txt" # Wrong ID
    meta_data = "Metadaten_Geographie_00001.txt"
    
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr(valid_data, "Valid Data")
        zf.writestr(other_data, "Wrong ID Data")
        zf.writestr(meta_data, "Meta Data")
        
    # Act: Unpack with unpack_hist_data_only=True
    downloader.extract(zip_files=[zip_name], unpack_hist_data_only=True)
    
    extract_folder = tmp_path / "extracted" / "tageswerte_KL_00001_19370101_19860630_hist"
    
    assert (extract_folder / valid_data).exists()
    assert not (extract_folder / meta_data).exists()
    assert not (extract_folder / other_data).exists()


def test_read_data(downloader, tmp_path):
    # Setup: Create extracted folder structure and a data csv file
    folder_name = "tageswerte_KL_00001_19370101_19860630_hist"
    folder_path = tmp_path / "extracted" / folder_name
    folder_path.mkdir(parents=True)
    
    file_name = "produkt_klima_tag_19370101_19860630_00001.txt"
    file_path = folder_path / file_name
    
    # CSV content with ; separator and some dummy columns
    # STATIONS_ID; MESS_DATUM; QN_3;   FX;  FM; QN_4; RSK; RSKF; SDK; SHK_TAG;  NM; VPM;  PM; TMK; UPM; TXK; TNK; TGK; eor
    content = (
        "STATIONS_ID; MESS_DATUM; QN_3; FX; FM; eor\n"
        "          1; 19370101;    1; 10.0; 5.0; eor\n"
        "          1; 19370102;    1; 12.0; 6.0; eor\n"
    )
    file_path.write_text(content)
    
    # Act
    # Passing the zip file name (or list) prompts read_data to look into corresponding folder
    zip_name = folder_name + ".zip" 
    results = downloader.read_data(zip_files=[zip_name], save_as_csv=True)
    
    # Assert
    assert file_name in results
    df = results[file_name]
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "eor" not in df.columns # Should be dropped
    assert "MESS_DATUM" in df.columns
    
    # Check CSV save
    csv_path = folder_path / file_name.replace(".txt", ".csv")
    assert csv_path.exists()
    
    # Verify CSV content structure
    saved_df = pd.read_csv(csv_path, sep=";")
    assert len(saved_df) == 2
    assert "MESS_DATUM" in saved_df.columns
