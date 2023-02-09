
from time import sleep

from .bioproject import BioProject
from .setup_logging import logger


def create_geoseeq_group_from_bioproj(org, bioproj_accession, sleep_time=2):
    """Create a GeoSeeq project from an NCBI bioproject. Return the GeoSeeq project."""
    bioproj = BioProject(bioproj_accession)
    logger.info(bioproj)
    grp = bioproj.geoseeq_obj(org)
    grp.idem()
    logger.info(grp)

    samples, ars, fields = [], [], []
    for biosample in bioproj.biosamples():
        logger.info(biosample)
        sample = biosample.geoseeq_obj(grp)
        logger.info(sample)
        samples.append(sample)
        sleep(sleep_time)
        for sra in biosample.sra():
            logger.info(sra)
            ar, field = sra.geoseeq_obj(sample)
            logger.info(ar)
            logger.info(field)
            ars.append(ar)
            fields.append(field)
            sleep(sleep_time)

    for sample in samples:
        sample.idem()

    for ar in ars:
        ar.idem()

    for field in fields:
        field.idem()

    return grp
