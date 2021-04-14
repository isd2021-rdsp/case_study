import logging

import apitep_core
import pandas as pd
from apitep_utils import ETL

log = logging.getLogger(__name__)


class WorkHistoryETL(ETL):

    @ETL.stopwatch
    def process(self):
        log.info("Process Work History data")
        log.debug("WorkHistoryETL.process()")

        pr_univ_degrees = pd.read_csv(
            '../../../../Data/Processed/pr_univ_degrees.csv',
            header=0,
            sep=apitep_core.CSV_SEPARATOR,
            na_values=['', ' ', 'nan'])
        rows_before = len(self.input_df.index)
        self.input_df = self.input_df[self.input_df['DNI'].isin(pr_univ_degrees['DNI'])]
        rows_after = len(self.input_df.index)

        self.changes["records_deleted"] = rows_before - rows_after

        self.output_df = self.input_df


def main():
    """
    Set logging up. Extract, transform, and load data.
    """

    logging.basicConfig(
        filename="debug.log",
        level=logging.DEBUG,
        format="%(asctime)-15s %(levelname)8s %(name)s %(message)s")
    logging.getLogger("matplotlib").setLevel(logging.ERROR)

    log.info("Start ETL_Work_History")
    log.debug("main()")

    etl = WorkHistoryETL(
        input_separator="|",
        output_separator="|"
    )
    etl.parse_arguments()
    etl.load()
    etl.process()
    etl.save()
    etl.log_changes()

    # work_history = pd.read_csv(
    #     '../../../../Data/Raw/work_history.csv',
    #     header=0,
    #     sep=apitep_core.CSV_SEPARATOR,
    #     na_values=['', ' ', 'nan'])
    #
    # pr_univ_degrees = pd.read_csv(
    #     '../../../../Data/Processed/pr_univ_degrees.csv',
    #     header=0,
    #     sep=apitep_core.CSV_SEPARATOR,
    #     na_values=['', ' ', 'nan'])
    #
    # training_courses = work_history[work_history['DNI'].isin(pr_univ_degrees['DNI'])]
    #
    # training_courses.to_csv(
    #     '../../../../Data/Processed/pr_work_history.csv',
    #     header=True,
    #     sep=apitep_core.CSV_SEPARATOR)


if __name__ == "__main__":
    main()
