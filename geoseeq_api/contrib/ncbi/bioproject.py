import xml.etree.ElementTree as ET

from Bio import Entrez


def recurse_xml_to_reactjs_safe_dict(v):
    out = {"tag": v.tag, "attributes": [v.attrib], "children": []}
    for child in v:
        out["children"].append(recurse_xml_to_reactjs_safe_dict(child))
    return out


def print_xml_recurse(v, pref=""):
    print(pref, v.tag, v.attrib)
    for child in v:
        print_xml_recurse(child, pref + "\t")


def xml_sra_file_to_blob(sra):
    out = {
        "__type__": "sra",
        "url": sra.attrib["url"],
        "md5": sra.attrib["md5"],
        "full_record": sra.attrib,
        "alternatives": [child.attrib for child in sra.iter("Alternatives")],
    }
    return out


class BioProject:
    def __init__(self, accession):
        self.accession = accession
        self._sra_records = None
        self.root = None

    def fetch(self):
        if self.root:
            return self
        handle = Entrez.efetch(db="bioproject", id=self.accession_num)
        self.root = ET.fromstring(handle.read())
        return self

    def metadata(self):
        self.fetch()
        out = {"ncbi_xml": ET.tostring(self.root, encoding="unicode", method="xml")}
        return out

    @property
    def accession_num(self):
        return self.accession.split("PRJNA")[-1]

    def biosamples(self):
        handle = Entrez.elink(
            dbfrom="bioproject", id=self.accession_num, linkname="bioproject_biosample"
        )
        results = Entrez.read(handle)
        out = []
        for el in results[0]["LinkSetDb"][0]["Link"]:
            out.append(BioSample(el["Id"], self))
        return out

    def geoseeq_obj(self, org):
        self.fetch()
        grp = org.sample_group(self.accession, metadata=self.metadata(), is_library=True)
        return grp

    def __str__(self):
        return f"<BioProject accession={self.accession} />"


class BioSample:
    def __init__(self, accession, bioproject=None):
        self.bioproject = bioproject
        self.accession = accession
        self.root = None

    @property
    def accession_num(self):
        return self.accession.split("SAMN")[-1]

    def fetch(self):
        if self.root:
            return self
        handle = Entrez.efetch(db="biosample", id=self.accession_num)
        self.root = ET.fromstring(handle.read())
        biosample_tag = self.root[0]
        self.accession = biosample_tag.attrib["accession"]
        return self

    def metadata(self):
        self.fetch()
        out = {"ncbi_xml": ET.tostring(self.root, encoding="unicode", method="xml")}
        return out

    def sra(self):
        handle = Entrez.elink(dbfrom="biosample", id=self.accession_num, linkname="biosample_sra")
        results = Entrez.read(handle)
        out = []
        for el in results[0]["LinkSetDb"][0]["Link"]:
            out.append(SRARecord(el["Id"], self))
        return out

    def geoseeq_obj(self, grp):
        self.fetch()
        sample = grp.sample(self.accession, metadata=self.metadata())
        return sample

    def __str__(self):
        return f"<BioSample accession={self.accession} />"


class SRARecord:
    def __init__(self, accession, biosample=None):
        self.biosample = biosample
        self.accession = accession
        self.root = None

    def fetch(self):
        if self.root:
            return self
        handle = Entrez.efetch(db="sra", id=self.accession_num)
        self.root = ET.fromstring(handle.read())
        return self

    def metadata(self):
        self.fetch()
        out = {"ncbi_xml": ET.tostring(self.root, encoding="unicode", method="xml")}
        return out

    def analysis_result_blob(self):
        self.fetch()
        sra_files = [sra_file for sra_file in self.root.iter("SRAFile")]
        assert len(sra_files) == 1
        sra_file = sra_files[0]
        blob = xml_sra_file_to_blob(sra_file)
        return blob

    @property
    def accession_num(self):
        return self.accession.split("PRJNA")[-1]

    def geoseeq_obj(self, sample):
        self.fetch()
        ar = sample.analysis_result(
            "raw::raw_reads", replicate=self.accession, metadata=self.metadata()
        )
        field = ar.field("sra_run", data=self.analysis_result_blob())
        return ar, field

    def __str__(self):
        return f"<SRARecord accession={self.accession} />"
