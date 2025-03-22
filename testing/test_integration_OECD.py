import unittest
from modules.utils.oecd import OECD
import modules.utils.dictionaries as dicts
import modules.databases.db_connection as db
import sqlalchemy
from modules.testing.aa_cleaner.clean_test_database import (
    clean_test_database,
    reset_test_tables,
)

path_db_env = "./modules/databases/.env"
test_oecd = True


@unittest.skipIf(not test_oecd, "skipping the testing of OECD")
class OECD_SQL(unittest.TestCase):
    """
    Integration test of OECD with the database (SQL backend), checking
    that OECD can download and update the database.
    """

    @classmethod
    def setUpClass(cls):
        """
        Setup of symbols, paths, objects, etc. needed for OECD and
        database interactions:
            - All tables in the "testing_" database are truncated
            and verified to be empty
        """
        # 0: Initialisation
        # 0.1: initialise database and OECD
        db.load_env_variables(path=path_db_env)
        cls.engine = db.get_engine("testing_")
        cls.oecd = OECD(silent=False, task_queuing="celery_submit", start_celery=True)
        # 0.2: delete all tables in the testing database
        reset_test_tables(engine=cls.engine, confirm=True)

    @classmethod
    def tearDownClass(cls):
        """
        Dispose SQL engine and kill celery workers.
        """
        cls.engine.dispose(close=True)
        # 3: kill all celery instances
        cls.oecd.celery_workers_stop()

    def test1_download_all_data(self):
        # 0: download
        d0 = self.oecd.download_all_data()
        self.assertEqual(len(d0), 6)

        # 1: upload
        self.oecd.update_db(d0, "testing_")

        # 2: get row numbers, assert they are larger than zero
        d_rows = {}
        with self.engine.connect() as connection:
            md = sqlalchemy.MetaData()
            md.reflect(bind=self.engine)
            for table in md.tables:
                # parse query
                query = f"SELECT COUNT(*) FROM `{table}`;"
                # execute query
                result_proxy = connection.execute(sqlalchemy.text(query))
                # fetch the result
                result = result_proxy.fetchall()
                d_rows[table] = result[0][0]
                # expect the table to be non-empty if it is not the combined data table
                if table.find("data") < 0:
                    self.assertGreater(
                        d_rows[table], 0, msg=f"{table}: Expected data is missing"
                    )

        # 3: upload again and check that the row numbers did not change
        self.oecd.update_db(d0, "testing_")
        with self.engine.connect() as connection:
            md = sqlalchemy.MetaData()
            md.reflect(bind=self.engine)
            for table in md.tables:
                # parse query
                query = f"SELECT COUNT(*) FROM `{table}`;"
                # execute query
                result_proxy = connection.execute(sqlalchemy.text(query))
                # fetch the result
                result = result_proxy.fetchall()
                self.assertEqual(d_rows[table], result[0][0])


if __name__ == "__main__":
    unittest.main()
