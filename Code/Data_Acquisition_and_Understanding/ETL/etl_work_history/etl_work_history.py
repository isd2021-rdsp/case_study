import pandas as pd
import apitep_core
from apitep_utils import ETL


def main():
    work_history = pd.read_csv(
        '../../../../Data/Raw/work_history.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    pr_univ_degrees = pd.read_csv(
        '../../../../Data/Processed/pr_univ_degrees.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    training_courses = work_history[work_history['DNI'].isin(pr_univ_degrees['DNI'])]

    training_courses.to_csv(
        '../../../../Data/Processed/pr_work_history.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR)


if __name__ == "__main__":
    main()
