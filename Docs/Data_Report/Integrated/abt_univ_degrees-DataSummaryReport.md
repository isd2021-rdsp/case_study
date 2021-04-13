# Data Report of dataset Titulados
This file describe the contain of dataset abt_univ_degrees.csv 

## General summary of the data

This dataset integrates the information available for those who have completed a university degree
 from more personal information such as gender or age to whether they have any official language certification
 whether they have completed any training courses
 whether they have completed any higher education prior to university and their percentage of time worked.

## Target variable
Percentage of time worked (TTO_PCTO_DIAS_TRABAJADOS)

## Individual variables

- SEXO
- TIPO_ACCESO
- MUNICIPIO_FAMILIAR
- EDAD_FIN
- EXP_PREVIA
- FSUP_PREV
- TR_COURSE
- LANG_CERT

## Variable ranking
1. EDAD_FIN
2. SEXO
3. EXP_PREVIA
4. TIPO_ACCESO
5. TR_COURSE
6. LANG_CERT
7. FSUP_PREV
8. MUNICIPIO_FAMILIAR

## Relationship between explanatory variables and target variable
- EDAD_FIN: P-value of Spearman Test = 0.105
- EXP_PREVIA: P-value of dunn test = 9.156438e-18
- SEXO: P-value of Dunn test = 0.0043 
- TIPO_ACCESO: P-value of Dunn Test in any groups < 0.05
- TR_COURSE: P-value Kruskal Wallis = 0.03
- LANG_CERT: P-value of Kruskal Wallis = 0.17 
- FSUP_PREV: P-value of Rank Sum Test = 0.69
- MUNICIPIO_FAMILIAR: P-value of Dunn Test in the majority of groups > 0.05





