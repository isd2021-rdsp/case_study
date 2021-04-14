import apitep_core
import pandas as pd
import config as cfg
import data_format as df
import dask.dataframe as dd
import swifter
import multiprocessing
from dateutil import relativedelta as rd
from more_itertools import zip_equal as izip
import numpy as np

pr_work_history = ""
work_history_dni = ""
work_history_person = ""
pr_univ_degrees = ""
pr_prev_education = ""
dni = ""
model = ""
pr_langs_certs = ""
pr_training_courses = ""


def repair_overlaps(ffin_estudios, contract):
    global model
    global dni
    act_dni = contract.DNI
    if act_dni == dni and contract.FECHA_INICIO <= model.FECHA_FIN:
        if contract.FECHA_FIN > model.FECHA_FIN:
            work_history_person.loc[model.name, 'FECHA_FIN'] = contract.FECHA_FIN
            model.FECHA_FIN = contract.FECHA_FIN
            work_history_person.loc[contract.name] = model.copy()
        work_history_person.drop(contract.name, axis=0, inplace=True)
    elif act_dni == dni:
        model = contract.copy()
    else:
        if contract.FECHA_INICIO < ffin_estudios and contract.FECHA_FIN <= ffin_estudios:
            work_history_person.drop(contract.name, axis=0, inplace=True)
        elif contract.FECHA_INICIO < ffin_estudios:
            contract.FECHA_INICIO = ffin_estudios
            dni = contract.DNI
            model = contract.copy()
            work_history_person.loc[contract.name] = model.copy()
        else:
            dni = contract.DNI
            model = contract.copy()


def days_between(f_ini, f_fin):
    if not pd.isna(f_ini) and not pd.isna(f_fin):
        return abs((f_fin - f_ini).days) + 1
    else:
        return 0


def get_dias_trabajados(persona):
    global work_history_person

    work_history_person = work_history_dni[work_history_dni.DNI == persona.DNI]

    if len(work_history_person) > 0:
        work_history_person.apply(lambda func: repair_overlaps(ffin_estudios=persona.FFIN_ESTUDIOS, contract=func),
                                  axis=1)
        if len(work_history_person) > 0:
            work_history_person['DURACION_CONTRATO_DIAS'] = work_history_person.apply(lambda func: days_between(
                func.FECHA_INICIO, func.FECHA_FIN), axis=1)
            dias_trabajados = work_history_person.apply(lambda func: sum(
                work_history_person['DURACION_CONTRATO_DIAS']), axis=1)
            return dias_trabajados.values[0]
        else:
            return 0

    elif len(pr_work_history[pr_work_history.DNI == persona.DNI]) > 0:
        return -1
    else:
        return 0


def get_num_esuperiores(dni):
    pr_uex_dni = pr_univ_degrees[pr_univ_degrees['DNI'] == dni]
    num_estudios = len(pr_uex_dni)
    if num_estudios >= 3:
        for titulacion in pr_uex_dni.itertuples():
            if 'PCEO' in titulacion.TITULACION:
                num_estudios -= 1
    return num_estudios


def get_ffin_eb(persona):
    global pr_prev_education

    eb_cursados = pr_prev_education[pr_prev_education['DNI'] == persona.DNI]

    if len(eb_cursados) > 2:
        eb_sel = pr_prev_education[(pr_prev_education['TIPO_ESTUDIO'] != 'E.S.O. (LOE)') &
                                   (pr_prev_education['TIPO_ESTUDIO'] != 'Bachiller (LOE)')].iloc[0, :]
    elif len(eb_cursados) > 1:
        eb_sel = pr_prev_education[pr_prev_education['DNI'] == persona.DNI].iloc[1, :]
    else:
        eb_sel = pr_prev_education[pr_prev_education['DNI'] == persona.DNI].iloc[0, :]

    ffin_ult_eb = str(int(eb_sel['AÑO_TITULACION'])) + '-09-01'
    return ffin_ult_eb, eb_sel['TIPO_ESTUDIO'], eb_sel['DES_CENTRO']


def get_ffin_esup(persona):
    global pr_univ_degrees

    ult_esup = pr_univ_degrees[pr_univ_degrees['DNI'] == persona.DNI].iloc[0, :]
    ffin_ult_esup = '20' + str(ult_esup['CURSO_FIN']).split("-")[1] + '-09-01'
    return ffin_ult_esup, ult_esup['TIPO_ESTUDIO'], ult_esup['TIPO_ACCESO'], ult_esup['MUNICIPIO_FAMILIAR']


def get_tto_pcto_dias_trabajados(persona):
    if persona.DIAS_TRABAJADOS > 0:
        dias_posibles = (pd.to_datetime(cfg.FECHA_TERMINAL_str) -
                         persona.FFIN_ESTUDIOS).days + 1
        tto_pcto = (persona.DIAS_TRABAJADOS / dias_posibles) * 100

        return tto_pcto
    elif persona.DIAS_TRABAJADOS < 0:
        return -1
    else:
        return 0


def get_exp_prev(persona):
    global pr_work_history

    pr_hist_cont_persona = pr_work_history[(pr_work_history['DNI'] == persona.DNI) &
                                           (pr_work_history['FECHA_INICIO'] < persona.FFIN_ESTUDIOS)]
    return not pr_hist_cont_persona.empty


def get_eb(persona):
    eb_persona = pr_prev_education[(pr_prev_education['DNI'] == persona.DNI) & (pr_prev_education['TIPO_ESTUDIO'] !=
                                                                                'E.S.O.') &
                                   (pr_prev_education['TIPO_ESTUDIO'] != 'Bachiller')]
    return not eb_persona.empty


def main():
    global work_history_dni, pr_work_history, pr_univ_degrees, pr_prev_education, pr_langs_certs, pr_training_courses

    pr_univ_degrees = pd.read_csv(
        '../../../../Data/Processed/pr_univ_degrees.csv',
        sep=apitep_core.CSV_SEPARATOR,
        header=0, index_col=0)

    pr_prev_education = pd.read_csv(
        '../../../../Data/Processed/pr_prev_education.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])
    pr_langs_certs = pd.read_csv(
        '../../../../Data/Processed/pr_langs_certs.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])
    pr_training_courses = pd.read_csv(
        '../../../../Data/Processed/pr_training_courses.csv',
        header=0,
        sep=apitep_core.CSV_SEPARATOR,
        na_values=['', ' ', 'nan'])

    dask_pr_uex = dd.from_pandas(data=pr_univ_degrees, npartitions=multiprocessing.cpu_count())

    pr_univ_degrees = dask_pr_uex.groupby('DNI').apply(
        lambda x: x.sort_values(['CURSO_FIN'], ascending=True)
    ).compute()

    abt_univ_degrees = pr_univ_degrees[['DNI', 'FNAC', 'SEXO']]
    abt_univ_degrees['SEXO'] = abt_univ_degrees['SEXO'].swifter.set_npartitions(
        multiprocessing.cpu_count()).apply(lambda func: 'H' if (func == 'Hombre') else 'M')
    df.drop_duplicates_filter_by_column(abt_univ_degrees, ['DNI'])

    abt_univ_degrees['FFIN_ESTUDIOS'], abt_univ_degrees['TIPO_ESTUDIO'], abt_univ_degrees['TIPO_ACCESO'], \
    abt_univ_degrees['MUNICIPIO_FAMILIAR'] = izip(*abt_univ_degrees.swifter.set_npartitions(
        multiprocessing.cpu_count()).apply(lambda func:
                                           get_ffin_esup(func),
                                           axis=1))

    abt_univ_degrees.reset_index(drop=True, inplace=True)

    abt_univ_degrees['FFIN_ESTUDIOS'] = pd.to_datetime(abt_univ_degrees['FFIN_ESTUDIOS'])

    global pr_work_history

    pr_work_history = pd.read_csv(
        '../../../../Data/Processed/pr_work_history.csv',
        sep=apitep_core.CSV_SEPARATOR,
        header=0)

    pr_work_history.rename(columns={"Unnamed: 0": "INDEX"}, inplace=True)
    pr_work_history['FECHA_INICIO'] = pd.to_datetime(pr_work_history['FECHA_INICIO'])
    pr_work_history['FECHA_FIN'] = pd.to_datetime(pr_work_history['FECHA_FIN'])

    dask_historial_contratos = dd.from_pandas(data=pr_work_history, npartitions=multiprocessing.cpu_count())

    global work_history_dni

    work_history_dni = dask_historial_contratos.groupby('DNI').apply(
        lambda x: x.sort_values(['FECHA_INICIO',
                                 'FECHA_FIN'],
                                ascending=True)) \
        .compute()

    work_history_dni.set_index('INDEX', inplace=True)

    abt_univ_degrees['DIAS_TRABAJADOS'] = 0
    abt_univ_degrees['DIAS_TRABAJADOS'] = abt_univ_degrees.apply(
        lambda func: get_dias_trabajados(func),
        axis=1)
    abt_univ_degrees['DIAS_TRABAJADOS'] = abt_univ_degrees['DIAS_TRABAJADOS'].astype('float')

    abt_univ_degrees['TTO_PCTO_DIAS_TRABAJADOS'] = 0
    abt_univ_degrees['TTO_PCTO_DIAS_TRABAJADOS'] = abt_univ_degrees.apply(
        lambda func: get_tto_pcto_dias_trabajados(func),
        axis=1)

    abt_univ_degrees['FNAC'] = abt_univ_degrees['FNAC'].apply(lambda x: '1993-05-11' if '193' in x else x)
    abt_univ_degrees['FFIN_ESTUDIOS'] = pd.to_datetime(abt_univ_degrees['FFIN_ESTUDIOS'])
    abt_univ_degrees['FNAC'] = pd.to_datetime(abt_univ_degrees['FNAC'])

    abt_univ_degrees['EDAD_FIN'] = 0
    abt_univ_degrees['EDAD_FIN'] = abt_univ_degrees.apply(
        lambda func: rd.relativedelta(func.FFIN_ESTUDIOS, func.FNAC).years,
        axis=1)
    abt_univ_degrees['EDAD_FIN'] = abt_univ_degrees['EDAD_FIN'].apply(lambda func: np.nan if func < 22 else func)

    abt_univ_degrees['EDAD_FIN'] = abt_univ_degrees.swifter.set_npartitions(
        multiprocessing.cpu_count()).apply(lambda func: df.imputation_func(func, 9, abt_univ_degrees, 'TIPO_ESTUDIO',
                                                                           func.TIPO_ESTUDIO), axis=1
                                           )

    abt_univ_degrees['EXP_PREVIA'] = False
    abt_univ_degrees['EXP_PREVIA'] = abt_univ_degrees.apply(get_exp_prev, axis=1)
    abt_univ_degrees['FSUP_PREV'] = False
    abt_univ_degrees['FSUP_PREV'] = abt_univ_degrees.apply(get_eb, axis=1)
    abt_univ_degrees['LANG_CERTS'] = False
    abt_univ_degrees['LANG_CERTS'] = abt_univ_degrees['DNI'].apply(
        lambda func: not (pr_langs_certs[pr_langs_certs['DNI'] == func]).empty)
    abt_univ_degrees['TR_COURSE'] = abt_univ_degrees['DNI'].apply(
        lambda func: not (pr_training_courses[pr_training_courses['DNI'] == func]).empty)

    final_columns = ['DNI', 'SEXO', 'TIPO_ACCESO',
       'MUNICIPIO_FAMILIAR', 'TTO_PCTO_DIAS_TRABAJADOS',
       'EDAD_FIN', 'EXP_PREVIA', 'FSUP_PREV', 'LANG_CERTS', 'TR_COURSE']

    abt_univ_degrees = abt_univ_degrees[final_columns]

    abt_univ_degrees.to_csv(
        '../../../../Data/Integrated/abt_univ_degrees.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR)
    df.generate_profile(abt_univ_degrees, 'abt_univ_degrees')


if __name__ == "__main__":
    main()
