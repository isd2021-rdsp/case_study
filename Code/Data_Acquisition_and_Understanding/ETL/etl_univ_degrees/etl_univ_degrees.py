import logging
import swifter
import numpy as np
from apitep_utils import Date
from apitep_utils import ETL

log = logging.getLogger(__name__)


class UExETL(ETL):
    BIRTH_DATE_ORIGINAL_FORMATTED = "Fecha nacimiento_FORMATTED"

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

    changes = {
        "id_card_formatted": 0,
        "no_birth_date_records_dropped": 0,
        "birth_dates_formatted": 0,
        "access_subtype_fixed": 0,
        "id_cards_encrypted": 0,
        "duplicates_deleted": 0,
        "number_of_studies_updated": 0,
        "columns_renamed": 0
    }

    @ETL.stopwatch
    def process(self):
        """
        Process UEx data.
        """

        log.info("Process UEx data")
        log.debug("UExETL.process()")

        # delete when birth date is empty
        rows_before = len(self.input_df.index)
        self.input_df[UExETL.BIRTH_DATE_PROCESSED].replace("", np.nan, inplace=True)
        self.input_df.dropna(subset=[UExETL.BIRTH_DATE_PROCESSED], inplace=True)
        rows_after = len(self.input_df.index)
        self.changes["no_birth_date_records_dropped"] = rows_before - rows_after

        # change birth date format (?)
        input_date_format = "%d/%m/%Y"
        self.input_df[UExETL.BIRTH_DATE_ORIGINAL_FORMATTED] = \
            self.input_df[UExETL.BIRTH_DATE_PROCESSED].swifter.progress_bar(False).apply(
                Date.change_format,
                args=(input_date_format,))
        self.changes["birth_dates_formatted"] = self.replace_column(
            source_column=UExETL.BIRTH_DATE_ORIGINAL_FORMATTED,
            destination_column=UExETL.BIRTH_DATE_PROCESSED
        )

        # delete duplicates
        rows_before = len(self.input_df.index)
        self.input_df.drop_duplicates(
            subset=[
                UExETL.IDENTITY_NUMBER_PROCESSED,
                UExETL.TITLE_PROCESSED],
            keep="first",
            inplace=True)
        rows_after = len(self.input_df.index)
        self.changes["duplicates_deleted"] = rows_before - rows_after

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
