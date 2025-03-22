import unittest
from modules.oecd import OECD
import modules.utils.dictionaries as dicts
import pandas as pd
from celery.result import AsyncResult
import os

path_db_env = os.getcwd()
test_oecd = True


@unittest.skipIf(not test_oecd, "skipping the testing of OECD")
class OECDClass(unittest.TestCase):
    """
    Unit test of OECD
    """

    def test_legacy(self):
        """
        Legacy task querying.
        """
        # 0: instantiate
        self.assertIsNotNone(
            OECD(silent=False, task_queuing="legacy", start_celery=False)
        )
        oecd = OECD(
            silent=False,
            task_queuing="legacy",
            start_celery=False,
            date_start="2021-01-01",
        )
        self.assertIsNone(oecd.worker_processes)
        self.assertIsNone(oecd.tasks)

        # 1: test data download methods
        # KEI
        df_out = oecd.key_economic_indicators(countries="USA")
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.key_economic_indicators(
            countries="USA", return_key=True, return_df_expected_columns_if_empty=True
        )
        self.assertEqual(str_out, "OECD-KEI")
        # BTS
        df_out = oecd.business_tendency_surveys(countries="USA")
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.business_tendency_surveys(
            countries="USA", return_key=True, return_df_expected_columns_if_empty=True
        )
        self.assertEqual(str_out, "OECD-BTS")
        # CLI
        df_out = oecd.composite_leading_indicators(countries="USA")
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.composite_leading_indicators(
            countries="USA", return_key=True, return_df_expected_columns_if_empty=True
        )
        self.assertEqual(str_out, "OECD-CLI")
        # FI
        df_out = oecd.financial_indicators(countries="USA")
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.financial_indicators(
            countries="USA", return_key=True, return_df_expected_columns_if_empty=True
        )
        self.assertEqual(str_out, "OECD-FI")
        # PS
        df_out = oecd.production_and_sales(countries="USA")
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.production_and_sales(
            countries="USA", return_key=True, return_df_expected_columns_if_empty=True
        )
        self.assertEqual(str_out, "OECD-PS")
        # CPI
        df_out = oecd.consumer_price_indices(countries="USA")
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.consumer_price_indices(
            countries="USA", return_key=True, return_df_expected_columns_if_empty=True
        )
        self.assertEqual(str_out, "OECD-CPI")

        # 2: None existing country, i.e. no data is return for a method
        oecd.time_wait_retry = 0.1
        oecd.retries = 1
        out = oecd.consumer_price_indices(
            countries="NOT-EXISTING", return_df_expected_columns_if_empty=False
        )
        self.assertIsNone(out)
        df_out = oecd.consumer_price_indices(
            countries="NOT-EXISTING", return_df_expected_columns_if_empty=True
        )
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertTrue(df_out.empty)

        # test that a non-existing data series is processed correctly
        out = oecd.oecd_query(
            dataflow_identifier="NON_EXISTING",
            filter_expression="SDAS",
            optional_parameters=None,
        )
        self.assertIsNone(out)

    def test_celery_wait(self):
        """
        Celery wait task querying.
        """
        # 0: instantiate
        oecd = OECD(silent=False, task_queuing="celery_wait", start_celery=True)
        self.assertIsNotNone(oecd.worker_processes)
        self.assertIsNotNone(oecd.tasks)

        # 1: test data download methods
        # KEI
        df_out = oecd.key_economic_indicators(countries=["EA19"])
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.key_economic_indicators(
            countries=["EA19"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-KEI")
        # BTS
        df_out = oecd.business_tendency_surveys(countries=["EA19"])
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.business_tendency_surveys(
            countries=["EA19"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-BTS")
        # CLI
        df_out = oecd.composite_leading_indicators(countries=["EA19"])
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.composite_leading_indicators(
            countries=["EA19"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-CLI")
        # FI
        df_out = oecd.financial_indicators(countries=["EA19"])
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.financial_indicators(
            countries=["EA19"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-FI")
        # PS
        df_out = oecd.production_and_sales(countries=["EA20"])
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.production_and_sales(
            countries=["EA20"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-PS")
        # CPI
        df_out = oecd.consumer_price_indices(countries=["EA20"])
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertGreaterEqual(len(df_out), 0)
        str_out = oecd.consumer_price_indices(
            countries=["EA20"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-CPI")

        # test that a non-existing data series is processed correctly
        out = oecd.oecd_query(
            dataflow_identifier="NON_EXISTING",
            filter_expression="SDAS",
            optional_parameters=None,
        )
        self.assertIsNone(out)

        # 2: None existing country, i.e. no data is return for a method
        out = oecd.consumer_price_indices(
            countries="NOT-EXISTING", return_df_expected_columns_if_empty=False
        )
        self.assertIsNone(out)
        df_out = oecd.consumer_price_indices(
            countries="NOT-EXISTING", return_df_expected_columns_if_empty=True
        )
        self.assertIsInstance(df_out, pd.DataFrame)
        self.assertTrue(df_out.empty)

        # 3: Test celery methods
        self.assertIsNone(
            oecd.celery_workers_stop(
                worker_processes=oecd.worker_processes, task_queuing=oecd.task_queuing
            )
        )
        self.assertIsNone(oecd.celery_app_initialise(tasks=None))
        self.assertIsNone(oecd.celery_app_initialise(tasks=oecd.tasks))
        self.assertIsNone(oecd.celery_workers_start(pool=None))
        self.assertIsNone(oecd.celery_workers_start(worker_processes=None))
        self.assertIsNone(
            oecd.celery_workers_start(worker_processes=oecd.worker_processes)
        )
        self.assertTrue(oecd.celery_workers_running(worker_processes=None))
        self.assertTrue(
            oecd.celery_workers_running(worker_processes=oecd.worker_processes)
        )

        # 4: stop celery
        oecd.celery_workers_stop()

    def test_celery_submit(self):
        """
        Celery with submit task querying.
        """
        # 0: instantiate
        oecd = OECD(silent=False, task_queuing="celery_submit", start_celery=True)
        self.assertIsNotNone(oecd.worker_processes)
        self.assertIsNotNone(oecd.tasks)

        # 1: test data download methods
        # KEI
        out = oecd.key_economic_indicators(countries=["EA19"])
        self.assertIsInstance(out, dict)
        self.assertIsInstance(dicts.dict_first_val(out), AsyncResult)
        str_out = oecd.key_economic_indicators(
            countries=["EA19"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-KEI")
        # BTS
        out = oecd.business_tendency_surveys(countries=["EA19"])
        self.assertIsInstance(out, dict)
        self.assertIsInstance(dicts.dict_first_val(out), AsyncResult)
        str_out = oecd.business_tendency_surveys(
            countries=["EA19"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-BTS")
        # CLI
        out = oecd.composite_leading_indicators(countries=["EA19"])
        self.assertIsInstance(out, dict)
        self.assertIsInstance(dicts.dict_first_val(out), AsyncResult)
        str_out = oecd.composite_leading_indicators(
            countries=["EA19"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-CLI")
        # FI
        out = oecd.financial_indicators(countries=["EA19"])
        self.assertIsInstance(out, dict)
        self.assertIsInstance(dicts.dict_first_val(out), AsyncResult)
        str_out = oecd.financial_indicators(
            countries=["EA19"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-FI")
        # PS
        out = oecd.production_and_sales(countries=["EA20"])
        self.assertIsInstance(out, dict)
        self.assertIsInstance(dicts.dict_first_val(out), AsyncResult)
        str_out = oecd.production_and_sales(
            countries=["EA20"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-PS")
        # CPI
        out = oecd.consumer_price_indices(countries=["EA20"])
        self.assertIsInstance(out, dict)
        self.assertIsInstance(dicts.dict_first_val(out), AsyncResult)
        str_out = oecd.consumer_price_indices(
            countries=["EA20"],
            return_key=True,
            return_df_expected_columns_if_empty=True,
        )
        self.assertEqual(str_out, "OECD-CPI")

        # 2: None existing country, i.e. no data is return for a method
        out = oecd.consumer_price_indices(countries="NOT-EXISTING")
        self.assertIsInstance(out, dict)
        self.assertIsInstance(dicts.dict_first_val(out), AsyncResult)

        # 3: Test celery methods
        self.assertIsNone(
            oecd.celery_workers_stop(
                worker_processes=oecd.worker_processes, task_queuing=oecd.task_queuing
            )
        )
        self.assertIsNone(oecd.celery_app_initialise(tasks=None))
        self.assertIsNone(oecd.celery_app_initialise(tasks=oecd.tasks))
        self.assertIsNone(oecd.celery_workers_start(pool=None))
        self.assertIsNone(oecd.celery_workers_start(worker_processes=None))
        self.assertIsNone(
            oecd.celery_workers_start(worker_processes=oecd.worker_processes)
        )
        self.assertTrue(oecd.celery_workers_running(worker_processes=None))
        self.assertTrue(
            oecd.celery_workers_running(worker_processes=oecd.worker_processes)
        )

        # 4: stop celery
        oecd.celery_workers_stop()

    def test_expected_exceptions(self):
        """
        Expected exceptions to be raised
        """
        self.assertRaises(AssertionError, OECD, **{"task_queuing": "INVALID METHOD"})
        self.assertRaises(AssertionError, OECD, **{"retries": "INVALID METHOD"})
        self.assertRaises(AssertionError, OECD, **{"time_wait_retry": "INVALID METHOD"})
        self.assertRaises(AssertionError, OECD, **{"date_start": "INVALID START DATE"})
        self.assertRaises(ValueError, OECD, **{"date_start": ["INVALID START DATE"]})


if __name__ == "__main__":
    unittest.main()
