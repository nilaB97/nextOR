import glob
import gzip
import os

import pandas as pd
import pyprind
from lxml import etree


QB_QUERY = {
    'ik': './Krankenhaus/Mehrere_Standorte/Krankenhauskontaktdaten/IK',

    #Krankenhausdaten
    'so_kh': './Krankenhaus/Mehrere_Standorte/Krankenhauskontaktdaten/Standortnummer',
    'so_alt_kh': './Krankenhaus/Mehrere_Standorte/Krankenhauskontaktdaten/Standortnummer_alt',
    'name_kh': './Krankenhaus/Mehrere_Standorte/Krankenhauskontaktdaten/Name',
    'plz_kh': './Krankenhaus/Mehrere_Standorte/Krankenhauskontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_kh': './Krankenhaus/Mehrere_Standorte/Krankenhauskontaktdaten/Kontakt_Zugang/Ort',
    'strasse_kh': './Krankenhaus/Mehrere_Standorte/Krankenhauskontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_kh': './Krankenhaus/Mehrere_Standorte/Krankenhauskontaktdaten/Kontakt_Zugang/Hausnummer',

    #Daten des Standortes
    'so_standort': './Krankenhaus/Mehrere_Standorte/Standortkontaktdaten/Standortnummer',
    'so_alt_standort': './Krankenhaus/Mehrere_Standorte/Standortkontaktdaten/Standortnummer_alt',
    'name_standort': './Krankenhaus/Mehrere_Standorte/Standortkontaktdaten/Name',
    'plz_standort': './Krankenhaus/Mehrere_Standorte/Standortkontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_standort': './Krankenhaus/Mehrere_Standorte/Standortkontaktdaten/Kontakt_Zugang/Ort',
    'strasse_standort': './Krankenhaus/Mehrere_Standorte/Standortkontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_standort': './Krankenhaus/Mehrere_Standorte/Standortkontaktdaten/Kontakt_Zugang/Hausnummer',

    #Krankenhausdaten: Ein Standort
    'ik_es': './Krankenhaus/Ein_Standort/Krankenhauskontaktdaten/IK',
    'so_es': './Krankenhaus/Ein_Standort/Krankenhauskontaktdaten/Standortnummer',
    'so_alt_es': './Krankenhaus/Ein_Standort/Krankenhauskontaktdaten/Standortnummer_alt',
    'name_es': './Krankenhaus/Ein_Standort/Krankenhauskontaktdaten/Name',
    'plz_es': './Krankenhaus/Ein_Standort/Krankenhauskontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_es': './Krankenhaus/Ein_Standort/Krankenhauskontaktdaten/Kontakt_Zugang/Ort',
    'strasse_es': './Krankenhaus/Ein_Standort/Krankenhauskontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_es': './Krankenhaus/Ein_Standort/Krankenhauskontaktdaten/Kontakt_Zugang/Hausnummer',

    # Krankenhausdaten: 2019, 2018, 2017 -> keine standorte angegeben?
    'ik_2019': './Krankenhaus/Krankenhauskontaktdaten/IK',
    'so_2019': './Krankenhaus/Krankenhauskontaktdaten/Standortnummer',
    'so_alt_2019': './Krankenhaus/Krankenhauskontaktdaten/Standortnummer_alt',
    'name_2019': './Krankenhaus/Krankenhauskontaktdaten/Name',
    'plz_2019': './Krankenhaus/Krankenhauskontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_2019': './Krankenhaus/Krankenhauskontaktdaten/Kontakt_Zugang/Ort',
    'strasse_2019': './Krankenhaus/Krankenhauskontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_2019': './Krankenhaus/Krankenhauskontaktdaten/Kontakt_Zugang/Hausnummer',

    # Krankenhausdaten Standortkontakt: 2019,2018, 2017 -> keine standorte angegeben?
    'ik_2019_so': './Krankenhaus/Standortkontaktdaten/IK',
    'so_2019_so': './Krankenhaus/Standortkontaktdaten/Standortnummer',
    'so_alt_2019_so': './Krankenhaus/Standortkontaktdaten/Standortnummer_alt',
    'name_2019_so': './Krankenhaus/Standortkontaktdaten/Name',
    'plz_2019_so': './Krankenhaus/Standortkontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_2019_so': './Krankenhaus/Standortkontaktdaten/Kontakt_Zugang/Ort',
    'strasse_2019_so': './Krankenhaus/Standortkontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_2019_so': './Krankenhaus/Standortkontaktdaten/Kontakt_Zugang/Hausnummer',

    # Krankenhausdaten: 2016, 2015 -> Kontaktdaten
    'ik_2016': './Krankenhaus/Kontaktdaten/IK',
    'so_2016': './Krankenhaus/Kontaktdaten/Standortnummer',
    'so_alt_2016': './Krankenhaus/Kontaktdaten/Standortnummer_alt',
    'name_2016': './Krankenhaus/Kontaktdaten/Name',
    'plz_2016': './Krankenhaus/Kontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_2016': './Krankenhaus/Kontaktdaten/Kontakt_Zugang/Ort',
    'strasse_2016': './Krankenhaus/Kontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_2016': './Krankenhaus/Kontaktdaten/Kontakt_Zugang/Hausnummer',

    # Krankenhausdaten Standortkontakt: 2016, 2015 -> komplett anders, WTF!!!
    'ik_2016_so': './Standort_dieses_Berichts/Kontaktdaten/IK',
    'so_2016_so': './Standort_dieses_Berichts/Kontaktdaten/Standortnummer',
    'so_alt_2016_so': './Standort_dieses_Berichts/Kontaktdaten/Standortnummer_alt',
    'name_2016_so': './Standort_dieses_Berichts/Kontaktdaten/Name',
    'plz_2016_so': './Standort_dieses_Berichts/Kontaktdaten/Kontakt_Zugang/Postleitzahl',
    'ort_2016_so': './Standort_dieses_Berichts/Kontaktdaten/Kontakt_Zugang/Ort',
    'strasse_2016_so': './Standort_dieses_Berichts/Kontaktdaten/Kontakt_Zugang/Strasse',
    'hausnr_2016_so': './Standort_dieses_Berichts/Kontaktdaten/Kontakt_Zugang/Hausnummer',


    #Daten über die Trägerschaft
    'traeger': './Krankenhaustraeger/Name',
    'traeger_art': './Krankenhaustraeger/Krankenhaustraeger_Art/Art',

    #Fallzahlen
    'fallzahlen_vollstationaer' : './Fallzahlen/Vollstationaere_Fallzahl',
    'fallzahlen_teilstationaer' :'./Fallzahlen/Teilstationaere_Fallzahl',
    'fallzahlen_ambulant' :'./Fallzahlen/Ambulante_Fallzahl',
    'fallzahlen_StaeB' :'./Fallzahlen/StaeB_Fallzahl',

    #Personal des Krankenhauses
    #Ärzte
    'aerzte_ohne_belegaerzte' :   './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Belegaerzte/Personalerfassung/Anzahl_VK',
    'aerzte_ohne_belegaerzte_ambulant': './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Belegaerzte/Personalerfassung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'aerzte_ohne_belegaerzte_stationaer': './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Belegaerzte/Personalerfassung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',

    #Fachärzte
    'fachaerzte_ohne_belegaerzte': './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Belegaerzte/Fachaerzte/Personalerfassung/Anzahl_VK',
    'fachaerzte_ohne_belegaerzte_ambulant': './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Belegaerzte/Fachaerzte/Personalerfassung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'fachaerzte_ohne_belegaerzte_stationaer': './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Belegaerzte/Fachaerzte/Personalerfassung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',

    #Belegärzte
    'belegaerzte': './Personal_des_Krankenhauses/Aerzte/Belegaerzte/Anzahl',

    #Ärzte ohne Fachabteilungszuordnung
    'aerzte_ohne_fachabteilungszuordnung': './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Fachabteilungszuordnung/Personalerfassung/Anzahl_VK',
    'aerzte_ohne_fachabteilungszuordnung_ambulant': './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Fachabteilungszuordnung/Personalerfassung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'aerzte_ohne_fachabteilungszuordnung_stationaer': './Personal_des_Krankenhauses/Aerzte/Aerzte_ohne_Fachabteilungszuordnung/Personalerfassung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',

    #Pflegekräfte
    'pflegekraefte' :   './Personal_des_Krankenhauses/Pflegekraefte/Gesundheits_Krankenpfleger/Personalerfassung/Anzahl_VK',
    'pflegekraefte_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Gesundheits_Krankenpfleger/Personalerfassung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'pflegekraefte_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Gesundheits_Krankenpfleger/Personalerfassung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',
    'pflegekraefte_ohne_fachabteilungszuordnung': './Personal_des_Krankenhauses/Pflegekraefte/Gesundheits_Krankenpfleger/Personalerfassung_ohne_Fachabteilungszuordnung/Anzahl_VK',
    'pflegekraefte_ohne_fachabteilungszuordnung_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Gesundheits_Krankenpfleger/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'pflegekraefte_ohne_fachabteilungszuordnung_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Gesundheits_Krankenpfleger/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',


    #Altenpfleger
    'altenpfleger' :   './Personal_des_Krankenhauses/Pflegekraefte/Altenpfleger/Personalerfassung/Anzahl_VK',
    'altenpfleger_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Altenpfleger/Personalerfassung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'altenpfleger_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Altenpfleger/Personalerfassung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',
    'altenpfleger_ohne_fachabteilungszuordnung': './Personal_des_Krankenhauses/Pflegekraefte/Altenpfleger/Personalerfassung_ohne_Fachabteilungszuordnung/Anzahl_VK',
    'altenpfleger_ohne_fachabteilungszuordnung_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Altenpfleger/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'altenpfleger_ohne_fachabteilungszuordnung_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Altenpfleger/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',

    #Krankenpflegehelfer
    'krankenpflegehelfer' :   './Personal_des_Krankenhauses/Pflegekraefte/Krankenpflegehelfer/Personalerfassung/Anzahl_VK',
    'krankenpflegehelfer_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Krankenpflegehelfer/Personalerfassung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'krankenpflegehelfer_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Krankenpflegehelfer/Personalerfassung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',
    'krankenpflegehelfer_ohne_fachabteilungszuordnung': './Personal_des_Krankenhauses/Pflegekraefte/Krankenpflegehelfer/Personalerfassung_ohne_Fachabteilungszuordnung/Anzahl_VK',
    'krankenpflegehelfer_ohne_fachabteilungszuordnung_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Krankenpflegehelfer/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'krankenpflegehelfer_ohne_fachabteilungszuordnung_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Krankenpflegehelfer/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',

    #Pflegehelfer
    'pflegehelfer' :   './Personal_des_Krankenhauses/Pflegekraefte/Pflegehelfer/Personalerfassung/Anzahl_VK',
    'pflegehelfer_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Pflegehelfer/Personalerfassung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'pflegehelfer_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Pflegehelfer/Personalerfassung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',
    'pflegehelfer_ohne_fachabteilungszuordnung': './Personal_des_Krankenhauses/Pflegekraefte/Pflegehelfer/Personalerfassung_ohne_Fachabteilungszuordnung/Anzahl_VK',
    'pflegehelfer_ohne_fachabteilungszuordnung_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Pflegehelfer/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'pflegehelfer_ohne_fachabteilungszuordnung_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Pflegehelfer/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',

    # Medizinische_Fachangestellte
    'medizinische_fachangestellte': './Personal_des_Krankenhauses/Pflegekraefte/Medizinische_Fachangestellte/Personalerfassung/Anzahl_VK',
    'medizinische_fachangestellte_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Medizinische_Fachangestellte/Personalerfassung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'medizinische_fachangestellte_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Medizinische_Fachangestellte/Personalerfassung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',
    'medizinische_fachangestellte_ohne_fachabteilungszuordnung': './Personal_des_Krankenhauses/Pflegekraefte/Medizinische_Fachangestellte/Personalerfassung_ohne_Fachabteilungszuordnung/Anzahl_VK',
    'medizinische_fachangestellte_ohne_fachabteilungszuordnung_ambulant': './Personal_des_Krankenhauses/Pflegekraefte/Medizinische_Fachangestellte/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Ambulante_Versorgung/Anzahl_VK',
    'medizinische_fachangestellte_ohne_fachabteilungszuordnung_stationaer': './Personal_des_Krankenhauses/Pflegekraefte/Pflegehelfer/Personalerfassung_ohne_Fachabteilungszuordnung/Versorgungsform/Stationaere_Versorgung/Anzahl_VK',

}


def get_root(path):
    if path.endswith('.gz'):
        with gzip.open(path) as f:
            return etree.parse(f)
    with open(path) as f:
        return etree.parse(f)


def get_paths(paths, exclude=None, include=None):
    for path in paths:
        if include is not None and not any(x in path for x in include):
            continue
        if exclude is not None and any(x in path for x in exclude):
            continue
        yield path


def apply_patches(df, path):
    patches = pd.read_csv(path)
    for _, patch in patches.iterrows():
        val = patch['value']
        if patch['type'] == 'int':
            val = int(val)
        df.loc[(df['path'] == patch['path']), patch['field']] = val
    return df


class PathIterator(object):
    def __init__(self, data_paths=None, exclude=None, include=None,
                 base_query=None, file_pattern='*xml.xml.gz'):
        self.paths = self.construct_paths(data_paths, exclude, include,
                                          file_pattern)
        self.base_query = base_query
        if self.base_query is None:
            self.base_query = {}

    def construct_paths(self, data_paths, exclude, include, file_pattern):
        paths = []
        for data_path in data_paths:
            names = glob.glob(os.path.join(data_path, file_pattern))
            paths.extend(list(get_paths(names, exclude=exclude, include=include)))
        return paths

    def get_val(self, node, xpath):
        matches = node.xpath(xpath)
        if not matches:
            return None
        return self.convert_match(matches[0])

    def convert_match(self, match):
        if match.text is None:
            return True
        val = match.text.strip()
        try:
            val = float(val.replace(',', '.'))
        except ValueError:
            pass
        return val

    def run_query(self, query):
        #Progress Bar
        print(len(self.paths))
        updater = pyprind.ProgPercent(len(self.paths))

        for path in self.paths:
            updater.update() #update progress bar for each query
            yield from self.run_query_for_path(path, query)

    def run_query_for_path(self, path, query):
        counter=0
        try:
            root = get_root(path)
        except Exception as e:
            #print(e)
            counter+=1
            return []
        #print(counter)
        new_query = dict(query)
        new_query.update(self.base_query)
        base_data = self.get_path_info(path)
        return self.run_sub_query(new_query, root, base_data=base_data)

    def run_sub_query(self, query, node, base_data=None):
        data = {}
        if base_data is not None:
            data.update(base_data)

        sub_xpath = None
        for key, val in query.items():
            if isinstance(val, dict):
                if sub_xpath:
                    raise ValueError('There must only be one deeper xpath per level (%s)' % sub_xpath)
                sub_xpath = key
            else:
                data[key] = self.get_val(node, val)

        if sub_xpath is not None:
            new_query = query[sub_xpath]
            for sub_node in node.xpath(sub_xpath):
                for sub_data in self.run_sub_query(new_query, node=sub_node):
                    new_data = dict(data)
                    new_data.update(sub_data)
                    yield new_data
        else:
            yield data

    def get_path_info(self, path):
        path_parts = path.split('/')[-1].split('-')
        return {
            'path': path,
            'path_ik': path_parts[0],
            'path_so': path_parts[1],
            'path_year': int(path_parts[2]),
        }


class QualityReports(object):
    def __init__(self, path='data/', years=[2014], base_query=QB_QUERY):
        self.path = path
        self.base_query = base_query
        self.years = years

    def query(self, query):
        data_paths = [self.get_year_path(year) for year in self.years]
        pit = PathIterator(data_paths=data_paths, exclude=('-99-',),
                           base_query=self.base_query)
        return pd.DataFrame(pit.run_query(query))

    def get_year_path(self, year):
        return os.path.join(self.path, 'base_%s' % year)


def apply_func(func, include=None, exclude=None, data_path='data/base_2015'):
    for path in glob.glob(data_path + '/*xml.xml.gz'):
        if include is not None and not any(x in path for x in include):
            continue
        if exclude is not None and any(x in path for x in exclude):
            continue
        root = get_root(path)
        yield func(root)
