import pandas as pd
import apitep_core
from apitep_utils import ETL


def main():
    langs_certs = pd.read_csv(
        '../../../../Data/Raw/languages_certs.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    pr_univ_degrees = pd.read_csv(
        '../../../../Data/Processed/pr_univ_degrees.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    langs_certs = langs_certs[langs_certs['DNI'].isin(pr_univ_degrees['DNI'])]

    langs_certs.to_csv(
        '../../../../Data/Processed/pr_langs_certs.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR)


if __name__ == "__main__":
    main()
