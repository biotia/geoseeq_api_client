
from .bioproject import BioProject
from ..tagging import Tag
from time import sleep
import logging
from .setup_logging import logger


def create_pangea_group_from_bioproj(org, bioproj_accession, sleep_time=2):
    """Add samples in an NCBI bioproject to this pangea group."""
    bioproj = BioProject(bioproj_accession)
    logger.info(bioproj)
    grp = bioproj.pangea_obj(org)
    grp.idem()
    bioproj_tag = Tag(org.knex, 'NCBI BioProject').idem()
    bioproj_tag(grp)
    logger.info(grp)

    samples, ars, fields = [], [], []
    for biosample in bioproj.biosamples():
        logger.info(biosample)
        sample = biosample.pangea_obj(grp)
        logger.info(sample)
        samples.append(sample)
        sleep(sleep_time)
        for sra in biosample.sra():
            logger.info(sra)
            ar, field = sra.pangea_obj(sample)
            logger.info(ar)
            logger.info(field)
            ars.append(ar)
            fields.append(field)
            sleep(sleep_time)

    biosample_tag = Tag(org.knex, 'NCBI BioSample').idem()
    for sample in samples:
        sample.idem()
        biosample_tag(sample)

    sra_tag = Tag(org.knex, 'NCBI SRA').idem()
    for ar in ars:
        ar.idem()
        sra_tag(ar)

    for field in fields:
        field.idem()

    return grp
