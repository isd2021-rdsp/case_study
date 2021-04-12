import pandas as pd
import numpy as np
import apitep_core
import get_apriori_rules


def main():
    analys_univ_degrees = pd.read_csv(
        '../../../../../Data/For_Analysis_and_Modeling/anlys_univ_degrees.csv',
        sep=apitep_core.CSV_SEPARATOR,
        header=0,
        index_col=0
    )

    tto_pcto_dias_trabajados_bckt_array = np.linspace(0, 100, 5)
    personas_esup_ml_25 = analys_univ_degrees.copy()
    personas_esup_ml_25['TTO_PCTO_DIAS_TRABAJADOS'] = pd.cut(personas_esup_ml_25['TTO_PCTO_DIAS_TRABAJADOS'],
                                                             tto_pcto_dias_trabajados_bckt_array)
    personas_esup_ml_25 = pd.get_dummies(data=personas_esup_ml_25,
                                         columns=['TIPO_ACCESO', 'EDAD_FIN', 'TTO_PCTO_DIAS_TRABAJADOS', 'SEXO',
                                                  'EXP_PREVIA'])

    results_0_25_10 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.1, 6,
                                                  '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(0.0, 25.0]'})''')

    results_0_25_5 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.05, 6,
                                                 '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(0.0, 25.0]'})''')

    results_0_25_2 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.02, 6,
                                                 '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(0.0, 25.0]'})''')

    results_0_25_1 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.01, 6,
                                                 '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(0.0, 25.0]'})''')

    results_0_25_10.to_csv(
        './Results/0_25/support_th_10/results_0_25_10.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False)

    results_0_25_5.to_csv(
        './Results/0_25/support_th_5/results_0_25_5.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False)

    results_0_25_2.to_csv(
        './Results/0_25/support_th_2/results_0_25_2.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False)
    results_0_25_1.to_csv(
        './Results/0_25/support_th_1/results_0_25_1.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False)

    results_26_50_10 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.1, 6,
                                                   '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(25.0, 50.0]'})''')

    results_26_50_5 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.05, 6,
                                                  '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(25.0, 50.0]'})''')

    results_26_50_2 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.02, 6,
                                                  '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(25.0, 50.0]'})''')

    results_26_50_1 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.01, 6,
                                                  '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(25.0, 50.0]'})''')

    results_26_50_10.to_csv(
        './Results/26_50/support_th_10/results_26_50_10.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )
    results_26_50_5.to_csv(
        './Results/26_50/support_th_5/results_26_50_5.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )
    results_26_50_2.to_csv(
        './Results/26_50/support_th_2/results_26_50_2.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )
    results_26_50_1.to_csv(
        './Results/26_50/support_th_1/results_26_50_1.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )

    results_51_75_5 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.05, 6,
                                                  '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(50.0, 75.0]'})''')

    results_51_75_2 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.02, 6,
                                                  '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(50.0, 75.0]'})''')

    results_51_75_1 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.01, 6,
                                                  '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(50.0, 75.0]'})''')

    results_51_75_5.to_csv(
        './Results/51_75/support_th_5/results_51_75_5.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )
    results_51_75_2.to_csv(
        './Results/51_75/support_th_2/results_51_75_2.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )
    results_51_75_1.to_csv(
        './Results/51_75/support_th_1/results_51_75_1.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )

    results_76_100_5 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.05, 6,
                                                   '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(75.0, 100.0]'})''')

    results_76_100_2 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.02, 6,
                                                   '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(75.0, 100.0]'})''')

    results_76_100_1 = get_apriori_rules.get_rules(personas_esup_ml_25, 0.01, 6,
                                                   '''frozenset({'TTO_PCTO_DIAS_TRABAJADOS_(75.0, 100.0]'})''')

    results_76_100_5.to_csv(
        './Results/76_100/support_th_5/results_76_100_5.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )
    results_76_100_2.to_csv(
        './Results/76_100/support_th_2/results_76_100_2.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )
    results_76_100_1.to_csv(
        './Results/76_100/support_th_1/results_76_100_1.csv',
        header=True,
        sep=apitep_core.CSV_SEPARATOR,
        index=False
    )


if __name__ == "__main__":
    main()
