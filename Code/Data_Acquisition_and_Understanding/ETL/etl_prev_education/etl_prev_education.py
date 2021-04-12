import pandas as pd
import apitep_core
from apitep_utils import ETL


def main():
    prev_education = pd.read_csv(
        '../../../../Data/Raw/prev_education.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    pr_univ_degrees = pd.read_csv(
        '../../../../Data/Processed/pr_univ_degrees.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    training_courses = prev_education[prev_education['DNI'].isin(pr_univ_degrees['DNI'])]

    training_courses.to_csv(
        '../../../../Data/Processed/pr_prev_education.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR)


if __name__ == "__main__":
    main()
