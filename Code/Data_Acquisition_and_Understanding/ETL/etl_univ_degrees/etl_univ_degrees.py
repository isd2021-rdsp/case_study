import logging

import numpy as np
from apitep_utils import Date
from apitep_utils import ETL

log = logging.getLogger(__name__)


class UExETL(ETL):
    COMPLETION_COURSE_ORIGINAL = "CursoFinalización"
    STUDY_TYPE_ORIGINAL = "TipoEstudio"
    TITLE_ORIGINAL = "Estudio"
    IDENTITY_NUMBER_ORIGINAL = "DNI"
    IDENTITY_NUMBER_ORIGINAL_PADDED = "DNI_PADDED"
    GENDER_ORIGINAL = "Sexo"
    BIRTH_DATE_ORIGINAL = "Fecha nacimiento"
    BIRTH_DATE_ORIGINAL_FORMATTED = "Fecha nacimiento_FORMATTED"
    START_COURSE_ORIGINAL = "CursoInicio"
    FAMILY_RESIDENCY_ORIGINAL = "Municipio/Provincia/País Residencia Familiar"
    SECONDARY_SCHOOL_ORIGINAL = "Centro Secundaria"
    PRE_REGISTRATION_ORDER_ORIGINAL = "OrdenPreinscripción"
    ACCESS_TYPE_ORIGINAL = "TipoAcceso"
    ACCESS_SUBTYPE_ORIGINAL = "SubTipoAcceso"
    ACCESS_SUBTYPE_ORIGINAL_FIXED = "SubTipoAcceso_FIXED"

    IDENTIFIER_PROCESSED = "ID"
    COMPLETION_COURSE_PROCESSED = "CURSO_FIN"
    STUDY_TYPE_PROCESSED = "TIPO_ESTUDIO"
    TITLE_PROCESSED = "TITULACION"
    IDENTITY_NUMBER_PROCESSED = "DNI"
    GENDER_PROCESSED = "SEXO"
    BIRTH_DATE_PROCESSED = "FNAC"
    START_COURSE_PROCESSED = "CURSO_INICIO"
    FAMILY_RESIDENCY_PROCESSED = "MUNICIPIO_FAMILIAR"
    SECONDARY_SCHOOL_PROCESSED = "CENTRO_SECUNDARIA"
    PRE_REGISTRATION_ORDER_PROCESSED = "ORDEN_PREINSCRIPCION"
    ACCESS_TYPE_PROCESSED = "TIPO_ACCESO"
    ACCESS_SUBTYPE_PROCESSED = "SUBTIPO_ACCESO"
    NUMBER_OF_STUDIES_PROCESSED = "NUM_ESTUDIOS"

    id_card_formatted = 0
    no_birth_date_records_dropped = 0
    birth_dates_formatted = 0
    access_subtype_fixed = 0
    id_cards_encrypted = 0
    duplicates_deleted = 0
    number_of_studies_updated = 0
    columns_renamed = 0

    @ETL.stopwatch
    def process(self):
        """
        Process UEx data.
        """

        log.info("Process UEx data")
        log.debug("UExETL.process()")

        # delete when birth date is empty
        rows_before = len(self.input_df.index)
        self.input_df[UExETL.BIRTH_DATE_ORIGINAL].replace("", np.nan, inplace=True)
        self.input_df.dropna(subset=[UExETL.BIRTH_DATE_ORIGINAL], inplace=True)
        rows_after = len(self.input_df.index)
        self.no_birth_date_records_dropped = rows_before - rows_after

        # change birth date format (?)
        input_date_format = "%d/%m/%Y"
        self.input_df[UExETL.BIRTH_DATE_ORIGINAL_FORMATTED] = \
            self.input_df[UExETL.BIRTH_DATE_ORIGINAL].swifter.progress_bar(False).apply(
                Date.change_format,
                args=(input_date_format,))
        self.birth_dates_formatted = self.replace_column(
            source_column=UExETL.BIRTH_DATE_ORIGINAL_FORMATTED,
            destination_column=UExETL.BIRTH_DATE_ORIGINAL
        )

        # delete duplicates
        rows_before = len(self.input_df.index)
        self.input_df.drop_duplicates(
            subset=[
                UExETL.IDENTITY_NUMBER_ORIGINAL,
                UExETL.TITLE_ORIGINAL],
            keep="first",
            inplace=True)
        rows_after = len(self.input_df.index)
        self.duplicates_deleted = rows_before - rows_after

        self.output_df = self.input_df

    @staticmethod
    def fix_access_subtype(access_subtype: str) -> str:
        """
        Fix miscellaneous typos in access subtype column.

        :param access_subtype: university access subtype.

        :return: formatted university access subtype.
        :rtype: str
        """

        # log.info("Fix university access subtype")
        # log.verbose(f"UExETL.fix_access_subtype("
        #             f"access_subtype={access_subtype})")

        if access_subtype == "ESTUDIOS EXTRANJEROS HOMOLOGADOS (MINISTERIO DE EDUACIÓN Y CIENCIA)":
            return "ESTUDIOS EXTRANJEROS HOMOLOGADOS (MINISTERIO DE EDUCACIÓN Y CIENCIA)"
        elif access_subtype == "Otros" or access_subtype == ".":
            return "Otros (sin especificar)"
        else:
            return access_subtype

    def log_changes(self):
        """
        Dump to log how many changes are made to UEx dataset.
        """

        log.info("Log dataset changes")
        log.debug("log_changes()")

        log.info(f"- id cards formatted: {self.id_card_formatted}")
        log.info(f"- rows with no birth date dropped: {self.no_birth_date_records_dropped}")
        log.info(f"- birth dates formatted: {self.birth_dates_formatted}")
        log.info(f"- access subtypes fixed: {self.access_subtype_fixed}")
        log.info(f"- id cards encrypted: {self.id_cards_encrypted}")
        log.info(f"- duplicates deleted: {self.duplicates_deleted}")
        log.info(f"- number of studies updated: {self.number_of_studies_updated}")
        log.info(f"- columns renamed: {UExETL.COLUMNS_TO_RENAME}")


def main():
    """
    Set logging up. Extract, transform, and load data.
    """

    logging.basicConfig(
        filename="debug.log",
        level=logging.DEBUG,
        format="%(asctime)-15s %(levelname)8s %(name)s %(message)s")
    logging.getLogger("matplotlib").setLevel(logging.ERROR)

    log.info("Start ETL_UEx")
    log.debug("main()")

    etl = UExETL(
        input_separator="|",
        output_separator="|"
    )
    etl.parse_arguments()
    etl.load()
    etl.process()
    etl.save()
    etl.log_changes()


if __name__ == "__main__":
    main()
