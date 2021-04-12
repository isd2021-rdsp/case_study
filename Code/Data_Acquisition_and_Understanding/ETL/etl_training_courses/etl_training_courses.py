import pandas as pd
import apitep_core
from apitep_utils import ETL


def main():
    training_courses = pd.read_csv(
        '../../../../Data/Raw/training_courses.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    pr_univ_degrees = pd.read_csv(
        '../../../../Data/Processed/pr_univ_degrees.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    training_courses = training_courses[training_courses['DNI'].isin(pr_univ_degrees['DNI'])]

    training_courses.to_csv(
        '../../../../Data/Processed/pr_training_courses.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR)


if __name__ == "__main__":
    main()
