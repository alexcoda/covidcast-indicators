from datetime import date
import os
import tempfile
from unittest.mock import patch

import numpy as np

from delphi_nowcast.data_containers import LocationSeries, SensorConfig
from delphi_nowcast.sensorization.sensor import historical_sensors, compute_sensors, _export_to_csv


class TestHisoricalSensors:

    @patch("delphi_nowcast.sensorization.sensor.get_historical_sensor_data")
    def test_historical_sensors_some_data(self, historical):
        """Test non empty data is returned for first two sensors."""
        historical.side_effect = [(LocationSeries(dates=[date(2020, 1, 1)], values=[2]), []),
                                  (LocationSeries(dates=[date(2020, 1, 3)], values=[4]), []),
                                  (LocationSeries(), [])]
        test_sensors = [SensorConfig("i", "j", "k", 3), SensorConfig("a", "b", "c", 1), SensorConfig("x", "y", "z", 2)]
        test_ground_truth = [LocationSeries("ca", "state")]
        assert historical_sensors(
            None, None, test_sensors, test_ground_truth) == {
            SensorConfig("i", "j", "k", 3): [LocationSeries(dates=[date(2020, 1, 1)], values=[2])],
            SensorConfig("a", "b", "c", 1): [LocationSeries(dates=[date(2020, 1, 3)], values=[4])]
        }

    @patch("delphi_nowcast.sensorization.sensor.get_historical_sensor_data")
    def test_historical_sensors_no_data(self, historical):
        """Test nothing returned for any sensor."""
        historical.return_value = (LocationSeries(), [])
        test_sensors = [SensorConfig("i", "j", "k", 3), SensorConfig("a", "b", "c", 1), SensorConfig("x", "y", "z", 2)]
        test_ground_truth = [LocationSeries("ca", "state")]
        assert historical_sensors(
            None, None, test_sensors, test_ground_truth) == {}


class TestComputeSensors:

    @patch("delphi_nowcast.sensorization.sensor.compute_ar_sensor")
    @patch("delphi_nowcast.sensorization.sensor.get_indicator_data")
    def test_compute_sensors_no_covariates(self, get_indicator_data, compute_ar_sensor):
        """Test only ground truth sensor is returned if no data is available to compute the rest."""
        get_indicator_data.return_value = {}
        compute_ar_sensor.return_value = 1.5
        test_sensors = [SensorConfig("a", "b", "c", 1), SensorConfig("x", "y", "z", 2)]
        test_ground_truth_sensor = SensorConfig("i", "j", "k", 3)
        test_ground_truth = [LocationSeries("ca", "state")]
        assert compute_sensors(
            date(2020, 5, 5), test_sensors, test_ground_truth_sensor, test_ground_truth, False
        ) == {
            SensorConfig("i", "j", "k", 3): [LocationSeries("ca", "state", [date(2020, 5, 2)], [1.5])],
        }

    @patch("delphi_nowcast.sensorization.sensor.compute_regression_sensor")
    @patch("delphi_nowcast.sensorization.sensor.compute_ar_sensor")
    @patch("delphi_nowcast.sensorization.sensor.get_indicator_data")
    def test_compute_sensors_covariates(self, get_indicator_data, compute_ar_sensor, compute_regression_sensor):
        """Test ground truth sensor and non-na regression sensor ar returned"""
        get_indicator_data.return_value = {("a", "b", "state", "ca"): ["placeholder"],
                                           ("x", "y", "state", "ca"): ["placeholder"]}
        compute_ar_sensor.return_value = 1.5
        compute_regression_sensor.side_effect = [2.5, np.nan]  # nan means 2nd sensor is skipped
        test_sensors = [SensorConfig("a", "b", "c", 1), SensorConfig("x", "y", "z", 2)]
        test_ground_truth_sensor = SensorConfig("i", "j", "k", 3)
        test_ground_truth = [LocationSeries("ca", "state")]
        assert compute_sensors(
            date(2020, 5, 5), test_sensors, test_ground_truth_sensor, test_ground_truth, False
        ) == {
            SensorConfig("i", "j", "k", 3): [LocationSeries("ca", "state", [date(2020, 5, 1)], [1.5])],
            SensorConfig("a", "b", "c", 1): [LocationSeries("ca", "state", [date(2020, 5, 4)], [2.5])],
        }


class TestExportToCSV:

    def test__export_to_csv(self):
        """Test export creates the right file and right contents."""
        test_sensor = SensorConfig(source="src",
                                   signal="sig",
                                   name="test",
                                   lag=4)
        test_value = LocationSeries("ca", "state", [date(2020, 1, 1)], [1.5])
        with tempfile.TemporaryDirectory() as tmpdir:
            out_files = _export_to_csv(test_value, test_sensor, date(2020, 1, 5), receiving_dir=tmpdir)
            assert len(out_files) == 1
            out_file = out_files[0]
            assert os.path.isfile(out_file)
            assert out_file.endswith("issue_20200105/src/20200101_state_sig.csv")
            with open(out_file) as f:
                assert f.read() == "sensor_name,geo_value,value\ntest,ca,1.5\n"


