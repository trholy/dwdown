import pytest
import requests
import responses
from unittest.mock import patch

from dwdown.download import MOSMIXDownloader

TEST_URL_BASE = "https://opendata.dwd.de/weather/local_forecasts/mos/"

@pytest.fixture
def mock_handlers():
    """Mock the handlers used by MOSMIX_Downloader."""
    with patch("dwdown.download.mosmix_download.SessionHandler") as MockSessionHandler, \
         patch("dwdown.download.mosmix_download.LogHandler") as MockLogHandler, \
         patch("dwdown.download.mosmix_download.FileHandler") as MockFileHandler, \
         patch("dwdown.download.mosmix_download.TimeHandler") as MockTimeHandler:
        
        # Setup SessionHandler to return a REAL session so 'responses' works
        session_instance = MockSessionHandler.return_value
        session_instance._session = requests.Session()
        session_instance.get_session.return_value = session_instance._session
        
        yield {
            "session": MockSessionHandler,
            "log": MockLogHandler,
            "file": MockFileHandler,
            "time": MockTimeHandler
        }

@pytest.fixture
def downloader(tmp_path, mock_handlers):
    """
    Creates a MOSMIXDownloader instance with temporary paths.
    """
    downloads_dir = tmp_path / "downloads"
    extracted_dir = tmp_path / "extracted"
    logs_dir = tmp_path / "logs"
    
    downloads_dir.mkdir()
    extracted_dir.mkdir()
    logs_dir.mkdir()

    return MOSMIXDownloader(
        mosmix_type="MOSMIX_L",
        files_path=str(downloads_dir),
        extracted_files_path=str(extracted_dir),
        log_files_path=str(logs_dir),
        delay=0,
        retry=0
    )

@responses.activate
def test_get_links_mosmix_l_single_station(downloader):
    # Test for MOSMIX_L single station URL construction
    station_id = "01001"
    station_url = f"{TEST_URL_BASE}MOSMIX_L/single_stations/{station_id}/kml/"
    
    sample_html = f'''
    <html><body><pre>
    <a href="MOSMIX_L_2026013015_{station_id}.kmz">file1.kmz</a>
    <a href="MOSMIX_L_2026013021_{station_id}.kmz">file2.kmz</a>
    </pre></body></html>
    '''
    responses.add(responses.GET, station_url, body=sample_html, status=200)
    
    # Mock filtering to return everything
    downloader._filehandler._simple_filename_filter.side_effect = lambda filenames, **kwargs: filenames
    
    links = downloader.get_links(station_ids=[station_id])
    
    assert len(links) == 2
    assert f"{station_url}MOSMIX_L_2026013015_{station_id}.kmz" in links

@responses.activate
def test_get_links_mosmix_s_all_stations(tmp_path, mock_handlers):
    # Test for MOSMIX_S (all stations)
    downloader = MOSMIXDownloader(
        mosmix_type="MOSMIX_S",
        files_path=str(tmp_path/"downloads"),
        extracted_files_path=str(tmp_path/"extracted"),
        log_files_path=str(tmp_path/"logs"),
        delay=0
    )
    
    all_stations_url = f"{TEST_URL_BASE}MOSMIX_S/all_stations/kml/"
    
    sample_html = '''
    <html><body><pre>
    <a href="MOSMIX_S_LATEST_240.kmz">latest.kmz</a>
    </pre></body></html>
    '''
    responses.add(responses.GET, all_stations_url, body=sample_html, status=200)
    
    downloader._filehandler._simple_filename_filter.side_effect = lambda filenames, **kwargs: filenames
    
    links = downloader.get_links()
    
    assert len(links) == 1
    assert f"{all_stations_url}MOSMIX_S_LATEST_240.kmz" in links

def test_parse_kml_simple(downloader, tmp_path):
    # Create a dummy KML file
    kml_content = """<?xml version="1.0" encoding="ISO-8859-1"?>
<kml xmlns:kml="http://www.opengis.net/kml/2.2">
  <Document>
    <dwd:ForecastTimeSteps xmlns:dwd="https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd">
      <dwd:TimeStep>2023-01-01T00:00:00.000Z</dwd:TimeStep>
      <dwd:TimeStep>2023-01-01T01:00:00.000Z</dwd:TimeStep>
    </dwd:ForecastTimeSteps>
    <kml:Placemark>
      <kml:name>01001</kml:name>
      <kml:description>STATION DESCRIPTION</kml:description>
      <kml:ExtendedData>
        <dwd:Forecast xmlns:dwd="https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd" dwd:elementName="TTT">
          <dwd:value> 273.15 274.15 </dwd:value>
        </dwd:Forecast>
      </kml:ExtendedData>
    </kml:Placemark>
  </Document>
</kml>
"""
    kml_file = tmp_path / "extracted" / "test.kml"
    (tmp_path / "extracted").mkdir(exist_ok=True)
    kml_file.write_text(kml_content)
    
    # Use private method to test parsing logic directly
    df = downloader._parse_kml(str(kml_file))
    
    assert df is not None
    assert not df.empty
    # Check Index: Station, Time
    assert "01001" in df.index.get_level_values("Station")
    assert "2023-01-01T00:00:00.000Z" in df.index.get_level_values("Time")
    # Check Columns
    assert "TTT" in df.columns
    # Check Value
    val = df.loc[("01001", "2023-01-01T00:00:00.000Z"), "TTT"]
    assert val == 273.15
