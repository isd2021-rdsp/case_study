import pandas as pd
import advanced_report as ar
import apitep_core
import config as cfg


def main():
    abt_univ_degrees = pd.read_csv(
        '../../../../../case_study/Data/Integrated/abt_univ_degrees.csv',
        sep=apitep_core.CSV_SEPARATOR,
        header=0,
        index_col=0)
    abt_univ_degrees = abt_univ_degrees[abt_univ_degrees['TTO_PCTO_DIAS_TRABAJADOS'] >= 0]
    abt_univ_degrees['SEXO'] = abt_univ_degrees['SEXO'].astype('category')
    abt_univ_degrees['TIPO_ESTUDIO'] = abt_univ_degrees['TIPO_ESTUDIO'].astype('category')
    abt_univ_degrees['TIPO_ACCESO'] = abt_univ_degrees['TIPO_ACCESO'].astype('category')
    abt_univ_degrees['MUNICIPIO_FAMILIAR'] = abt_univ_degrees['MUNICIPIO_FAMILIAR'].astype('category')
    abt_univ_degrees['EXP_PREVIA'] = abt_univ_degrees['EXP_PREVIA'].astype('category')
    abt_univ_degrees['FSUP_PREV'] = abt_univ_degrees['FSUP_PREV'].astype('category')

    abt_univ_degrees = abt_univ_degrees[abt_univ_degrees['TTO_PCTO_DIAS_TRABAJADOS'] > 0]

    ar.generate_advanced_report(abt_univ_degrees, 'abt_univ_degrees',
                                '/home/fran/Escritorio/ISD2021/Código experimento/case_study/Code/'
                                'Data_Acquisition_and_Understanding/Integration/int_univ_degrees'
                                '/advanced_report_univ_degrees')


if __name__ == "__main__":
    main()
