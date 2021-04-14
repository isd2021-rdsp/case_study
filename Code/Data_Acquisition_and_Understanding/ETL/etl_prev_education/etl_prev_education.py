import logging

import apitep_core
import pandas as pd
from apitep_utils import ETL

log = logging.getLogger(__name__)


class PrevEducationETL(ETL):

    @ETL.stopwatch
    def process(self):
        log.info("Process Prev Education data")
        log.debug("PrevEducationETL.process()")

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

    log.info("Start ETL_Prev_Education")
    log.debug("main()")

    etl = PrevEducationETL(
        input_separator="|",
        output_separator="|"
    )
    etl.parse_arguments()
    etl.load()
    etl.process()
    etl.save()
    etl.log_changes()

    # prev_education = pd.read_csv(
    #     '../../../../Data/Raw/prev_education.csv',
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
    # training_courses = prev_education[prev_education['DNI'].isin(pr_univ_degrees['DNI'])]
    #
    # training_courses.to_csv(
    #     '../../../../Data/Processed/pr_prev_education.csv',
    #     header=True,
    #     sep=apitep_core.CSV_SEPARATOR)


if __name__ == "__main__":
    main()
