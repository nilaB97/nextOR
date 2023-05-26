import pandas as pd


def add_nan_rows_ops(data, years):
    lambda_get_value_that_occurs_the_most = lambda x: x.value_counts().index[0]
    new_rows = pd.DataFrame(columns=data.columns)  # add new rows here
    # for all other datapoints add rows for the years we don't have with the average or value below/above
    for ik in data.ik.unique():
        tmp = data[data.ik == ik]
        tmp_years = tmp['year'].unique()
        if len(tmp_years) == 7:  # if we have all years break
            continue
        else:
            years = [2015, 2016, 2017, 2018, 2019, 2020, 2021]
            for year in years:
                if year not in tmp_years:
                    new_row = tmp.groupby(['ik', 'OPS_CODE'], as_index=False).aggregate({
                        'year': lambda x: year,
                        'Anzahl': 'median'
                    })
            new_rows = pd.concat([new_rows, new_row])

    new_rows.index.names = ['ik']
    return pd.concat([data, new_rows])


def aggregate_ops(data):
    data = data.groupby(['ik', 'year', 'OPS_CODE'], as_index=False).aggregate({

        'Anzahl': 'sum'
    })

    return data


def add_nan_rows(data, years):
    lambda_get_value_that_occurs_the_most = lambda x: x.value_counts().index[0]
    new_rows = pd.DataFrame(columns=data.columns)  # add new rows here
    # for all other datapoints add rows for the years we don't have with the average or value below/above
    for ik in data['ik'].unique():
        tmp = data[data.ik == ik]
        if tmp.shape[0] == 7:  # if we have all years break
            continue
        else:
            tmp_years = tmp['year'].values
            for year in years:
                if year not in tmp_years:
                    new_row = tmp.groupby(['ik'], as_index=False).aggregate({
                        'year': lambda x: year,
                        # Daten über die Trägerschaft
                        'traeger': lambda_get_value_that_occurs_the_most,
                        'traeger_art': pd.Series.mode,  # use mode here because sometimes we don't know this

                        # Fallzahlen
                        'fallzahlen_vollstationaer': 'median',
                        'fallzahlen_teilstationaer': 'median',
                        'fallzahlen_ambulant': 'median',
                        'fallzahlen_StaeB': 'median',

                        # Personal des Krankenhauses
                        # Ärzte
                        'aerzte_ohne_belegaerzte': 'median',
                        'aerzte_ohne_belegaerzte_ambulant': 'median',
                        'aerzte_ohne_belegaerzte_stationaer': 'median',

                        # Fachärzte
                        'fachaerzte_ohne_belegaerzte': 'median',
                        'fachaerzte_ohne_belegaerzte_ambulant': 'median',
                        'fachaerzte_ohne_belegaerzte_stationaer': 'median',

                        # Belegärzte
                        'belegaerzte': 'median',

                        # Ärzte ohne Fachabteilungszuordnung
                        'aerzte_ohne_fachabteilungszuordnung': 'median',
                        'aerzte_ohne_fachabteilungszuordnung_ambulant': 'median',
                        'aerzte_ohne_fachabteilungszuordnung_stationaer': 'median',

                        # Pflegekräfte
                        'pflegekraefte': 'median',
                        'pflegekraefte_ambulant': 'median',
                        'pflegekraefte_stationaer': 'median',
                        'pflegekraefte_ohne_fachabteilungszuordnung': 'median',
                        'pflegekraefte_ohne_fachabteilungszuordnung_ambulant': 'median',
                        'pflegekraefte_ohne_fachabteilungszuordnung_stationaer': 'median',

                        # Altenpfleger
                        'altenpfleger': 'median',
                        'altenpfleger_ambulant': 'median',
                        'altenpfleger_stationaer': 'median',
                        'altenpfleger_ohne_fachabteilungszuordnung': 'median',
                        'altenpfleger_ohne_fachabteilungszuordnung_ambulant': 'median',
                        'altenpfleger_ohne_fachabteilungszuordnung_stationaer': 'median',

                        # Krankenpflegehelfer
                        'krankenpflegehelfer': 'median',
                        'krankenpflegehelfer_ambulant': 'median',
                        'krankenpflegehelfer_stationaer': 'median',
                        'krankenpflegehelfer_ohne_fachabteilungszuordnung': 'median',
                        'krankenpflegehelfer_ohne_fachabteilungszuordnung_ambulant': 'median',
                        'krankenpflegehelfer_ohne_fachabteilungszuordnung_stationaer': 'median',

                        # Pflegehelfer
                        'pflegehelfer': 'median',
                        'pflegehelfer_ambulant': 'median',
                        'pflegehelfer_stationaer': 'median',
                        'pflegehelfer_ohne_fachabteilungszuordnung': 'median',
                        'pflegehelfer_ohne_fachabteilungszuordnung_ambulant': 'median',
                        'pflegehelfer_ohne_fachabteilungszuordnung_stationaer': 'median',

                        # Medizinische_Fachangestellte
                        'medizinische_fachangestellte': 'median',
                        'medizinische_fachangestellte_ambulant': 'median',
                        'medizinische_fachangestellte_stationaer': 'median',
                        'medizinische_fachangestellte_ohne_fachabteilungszuordnung': 'median',
                        'medizinische_fachangestellte_ohne_fachabteilungszuordnung_ambulant': 'median',
                        'medizinische_fachangestellte_ohne_fachabteilungszuordnung_stationaer': 'median',

                        # Adresse
                        'name': lambda_get_value_that_occurs_the_most,
                        'plz': lambda_get_value_that_occurs_the_most,
                        'ort': lambda_get_value_that_occurs_the_most,
                        'strasse': lambda_get_value_that_occurs_the_most,
                        'hausnr': lambda_get_value_that_occurs_the_most,
                        'address': lambda_get_value_that_occurs_the_most,
                        'ik-name': lambda_get_value_that_occurs_the_most,
                        'ik_bl': lambda_get_value_that_occurs_the_most,
                        'bundesland': lambda_get_value_that_occurs_the_most,

                    })
                    # new_row['year']=year
                    new_rows = pd.concat([new_rows,new_row])

    # new_rows.index.names=['ik']
    return pd.concat([data, new_rows])


def aggregate_data(data):
    lambda_get_value_that_occurs_the_most = lambda x: x.value_counts().index[0]
    data = data.groupby(['ik', 'year'], as_index=False).aggregate({
        # Daten über die Trägerschaft
        'traeger': lambda_get_value_that_occurs_the_most,
        'traeger_art': pd.Series.mode,  # use mode here because sometimes we don't know this

        # Fallzahlen
        'fallzahlen_vollstationaer': 'sum',
        'fallzahlen_teilstationaer': 'sum',
        'fallzahlen_ambulant': 'sum',
        'fallzahlen_StaeB': 'sum',

        # Personal des Krankenhauses
        # Ärzte
        'aerzte_ohne_belegaerzte': 'sum',
        'aerzte_ohne_belegaerzte_ambulant': 'sum',
        'aerzte_ohne_belegaerzte_stationaer': 'sum',

        # Fachärzte
        'fachaerzte_ohne_belegaerzte': 'sum',
        'fachaerzte_ohne_belegaerzte_ambulant': 'sum',
        'fachaerzte_ohne_belegaerzte_stationaer': 'sum',

        # Belegärzte
        'belegaerzte': 'sum',

        # Ärzte ohne Fachabteilungszuordnung
        'aerzte_ohne_fachabteilungszuordnung': 'sum',
        'aerzte_ohne_fachabteilungszuordnung_ambulant': 'sum',
        'aerzte_ohne_fachabteilungszuordnung_stationaer': 'sum',

        # Pflegekräfte
        'pflegekraefte': 'sum',
        'pflegekraefte_ambulant': 'sum',
        'pflegekraefte_stationaer': 'sum',
        'pflegekraefte_ohne_fachabteilungszuordnung': 'sum',
        'pflegekraefte_ohne_fachabteilungszuordnung_ambulant': 'sum',
        'pflegekraefte_ohne_fachabteilungszuordnung_stationaer': 'sum',

        # Altenpfleger
        'altenpfleger': 'sum',
        'altenpfleger_ambulant': 'sum',
        'altenpfleger_stationaer': 'sum',
        'altenpfleger_ohne_fachabteilungszuordnung': 'sum',
        'altenpfleger_ohne_fachabteilungszuordnung_ambulant': 'sum',
        'altenpfleger_ohne_fachabteilungszuordnung_stationaer': 'sum',

        # Krankenpflegehelfer
        'krankenpflegehelfer': 'sum',
        'krankenpflegehelfer_ambulant': 'sum',
        'krankenpflegehelfer_stationaer': 'sum',
        'krankenpflegehelfer_ohne_fachabteilungszuordnung': 'sum',
        'krankenpflegehelfer_ohne_fachabteilungszuordnung_ambulant': 'sum',
        'krankenpflegehelfer_ohne_fachabteilungszuordnung_stationaer': 'sum',

        # Pflegehelfer
        'pflegehelfer': 'sum',
        'pflegehelfer_ambulant': 'sum',
        'pflegehelfer_stationaer': 'sum',
        'pflegehelfer_ohne_fachabteilungszuordnung': 'sum',
        'pflegehelfer_ohne_fachabteilungszuordnung_ambulant': 'sum',
        'pflegehelfer_ohne_fachabteilungszuordnung_stationaer': 'sum',

        # Medizinische_Fachangestellte
        'medizinische_fachangestellte': 'sum',
        'medizinische_fachangestellte_ambulant': 'sum',
        'medizinische_fachangestellte_stationaer': 'sum',
        'medizinische_fachangestellte_ohne_fachabteilungszuordnung': 'sum',
        'medizinische_fachangestellte_ohne_fachabteilungszuordnung_ambulant': 'sum',
        'medizinische_fachangestellte_ohne_fachabteilungszuordnung_stationaer': 'sum',

        # Adresse
        'name': lambda_get_value_that_occurs_the_most,
        'plz': lambda_get_value_that_occurs_the_most,
        'ort': lambda_get_value_that_occurs_the_most,
        'strasse': lambda_get_value_that_occurs_the_most,
        'hausnr': lambda_get_value_that_occurs_the_most,
        'address': lambda_get_value_that_occurs_the_most,
        'ik-name': lambda_get_value_that_occurs_the_most,
        'ik_bl': lambda_get_value_that_occurs_the_most,
        'bundesland': lambda_get_value_that_occurs_the_most,

    })
    return data
