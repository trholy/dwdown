import unittest

from dwdown.data import MappingStore


class TestMappingStore(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment by creating an instance of MappingStore.
        """
        self.mapping_store = MappingStore()

    def test_get_mapping_dict(self):
        """
        Test the get_mapping_dict method to ensure it returns the correct mapping dictionary.
        """
        expected_mapping_dict = {
            'latitude': 'latitude',
            'longitude': 'longitude',
            'valid_time': 'valid_time',
            'aswdifd_s': 'ASWDIFD_S',
            'aswdir_s': 'ASWDIR_S',
            'cape_ml': 'CAPE_ML',
            'clch': 'CLCH',
            'clcl': 'CLCL',
            'clcm': 'CLCM',
            'clct': 'CLCT',
            'grau_gsp': 'tgrp',
            'h_snow': 'sde',
            'prg_gsp': 'PRG_GSP',
            'prr_gsp': 'lsrr',
            'prs_gsp': 'lssfr',
            'ps': 'sp',
            'q_sedim': 'Q_SEDIM',
            'rain_con': 'crr',
            'rain_gsp': 'lsrr',
            'relhum': 'r',
            'relhum_2m': 'r2',
            'rho_snow': 'rsn',
            'runoff_g': 'RUNOFF_G',
            'runoff_s': 'RUNOFF_S',
            'smi': 'SMI',
            'snow_con': 'csfwe',
            'snow_gsp': 'lsfwe',
            'soiltyp': 'SOILTYP',
            'td_2m': 'd2m',
            'tke': 'tke',
            'tmax_2m': 'mx2t',
            'tmin_2m': 'mn2t',
            'tot_prec': 'tp',
            'tqc': 'TQC',
            'tqg': 'TQG',
            'tqi': 'TQI',
            'tqr': 'tcolr',
            'tqs': 'tcols',
            'tqv': 'TQV',
            'twater': 'TWATER',
            't_2m': 't2m',
            't_g': 'T_G',
            't_snow': 'T_SNOW',
            't_so': 'T_SO',
            'u': 'u',
            'uh_max': 'UH_MAX',
            'uh_max_low': 'UH_MAX_LOW',
            'uh_max_med': 'UH_MAX_MED',
            'u_10m': 'u10',
            'v': 'v',
            'vis': 'vis',
            'vmax_10m': 'fg10',
            'vorw_ctmax': 'VORW_CTMAX',
            'v_10m': 'v10',
            'w': 'wz',
            'ww': 'WW',
            'w_ctmax': 'W_CTMAX',
            'w_so': 'W_SO',
            'w_so_ice': 'W_SO_ICE',
            'z0': 'fsr',
            'alhfl_s': 'avg_slhtf'}

        self.assertEqual(self.mapping_store.get_mapping_dict(), expected_mapping_dict)

    def test_get_mapping_dict_returns_copy(self):
        """
        Test that get_mapping_dict returns a copy of the internal dictionary, not the original reference.
        """
        mapping_dict = self.mapping_store.get_mapping_dict()
        mapping_dict['test_key'] = 'test_value'
        self.assertNotIn('test_key', self.mapping_store.get_mapping_dict())

    def test_get_mapping_dict_static_method(self):
        """
        Test the static method _get_mapping_dict to ensure it returns the correct mapping dictionary.
        """
        expected_mapping_dict = {
            'latitude': 'latitude',
            'longitude': 'longitude',
            'valid_time': 'valid_time',
            'aswdifd_s': 'ASWDIFD_S',
            'aswdir_s': 'ASWDIR_S',
            'cape_ml': 'CAPE_ML',
            'clch': 'CLCH',
            'clcl': 'CLCL',
            'clcm': 'CLCM',
            'clct': 'CLCT',
            'grau_gsp': 'tgrp',
            'h_snow': 'sde',
            'prg_gsp': 'PRG_GSP',
            'prr_gsp': 'lsrr',
            'prs_gsp': 'lssfr',
            'ps': 'sp',
            'q_sedim': 'Q_SEDIM',
            'rain_con': 'crr',
            'rain_gsp': 'lsrr',
            'relhum': 'r',
            'relhum_2m': 'r2',
            'rho_snow': 'rsn',
            'runoff_g': 'RUNOFF_G',
            'runoff_s': 'RUNOFF_S',
            'smi': 'SMI',
            'snow_con': 'csfwe',
            'snow_gsp': 'lsfwe',
            'soiltyp': 'SOILTYP',
            'td_2m': 'd2m',
            'tke': 'tke',
            'tmax_2m': 'mx2t',
            'tmin_2m': 'mn2t',
            'tot_prec': 'tp',
            'tqc': 'TQC',
            'tqg': 'TQG',
            'tqi': 'TQI',
            'tqr': 'tcolr',
            'tqs': 'tcols',
            'tqv': 'TQV',
            'twater': 'TWATER',
            't_2m': 't2m',
            't_g': 'T_G',
            't_snow': 'T_SNOW',
            't_so': 'T_SO',
            'u': 'u',
            'uh_max': 'UH_MAX',
            'uh_max_low': 'UH_MAX_LOW',
            'uh_max_med': 'UH_MAX_MED',
            'u_10m': 'u10',
            'v': 'v',
            'vis': 'vis',
            'vmax_10m': 'fg10',
            'vorw_ctmax': 'VORW_CTMAX',
            'v_10m': 'v10',
            'w': 'wz',
            'ww': 'WW',
            'w_ctmax': 'W_CTMAX',
            'w_so': 'W_SO',
            'w_so_ice': 'W_SO_ICE',
            'z0': 'fsr',
            'alhfl_s': 'avg_slhtf'}

        self.assertEqual(MappingStore._get_mapping_dict(), expected_mapping_dict)


if __name__ == '__main__':
    unittest.main()