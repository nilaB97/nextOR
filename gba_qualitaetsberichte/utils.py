import functools

import pandas as pd


def replace_and_save_nan(hygiene_df, val_cols, replacement=0, suffix='_original'):
    for c in val_cols:
        hygiene_df[c + suffix] = hygiene_df[c]
        hygiene_df[c] = hygiene_df[c].fillna(replacement)
    return hygiene_df


def choose_non_null(row, key=None):
    try:
        v = row['%s_standort' % key]
        if pd.notnull(v):
            return v
        v = row['%s_es' % key]
        if pd.notnull(v):
            return v
        v = row['%s_2019' % key]
        if pd.notnull(v):
            return v
        v = row['%s_2019_so' % key]
        if pd.notnull(v):
            return v
        v = row['%s_2016' % key]
        if pd.notnull(v):
            return v
        v = row['%s_2016_so' % key]
        if pd.notnull(v):
            return v
    except KeyError:
        pass
    return row['%s_kh' % key]


def fix_standort(hygiene_df):
    for k in ('ik','so','so_alt', 'name', 'plz', 'ort', 'strasse', 'hausnr'):
        if k=='ik':
            hygiene_df['ik'] = hygiene_df['ik'].apply(lambda x: 0 if pd.isnull(x) else int(x))
            hygiene_df['ik_es'] = hygiene_df['ik_es'].apply(lambda x: 0 if pd.isnull(x) else int(x))
            hygiene_df['ik_2019'] = hygiene_df['ik_2019'].apply(lambda x: 0 if pd.isnull(x) else int(x))
            # ik_2019 und ik_2019_so sollten dasselbe sein
            hygiene_df['ik_2016'] = hygiene_df['ik_2016'].apply(lambda x: 0 if pd.isnull(x) else int(x))
            # ik_2016 und ik_2016_so sollten dasselbe sein


            hygiene_df['ik'] = hygiene_df['ik'] + hygiene_df['ik_es'] + hygiene_df['ik_2019'] + hygiene_df['ik_2016']
            hygiene_df = hygiene_df.drop(['ik_es','ik_2019','ik_2019_so','ik_2016','ik_2016_so'], 1)
        else:
            hygiene_df[k] = hygiene_df.apply(functools.partial(choose_non_null, key=k), 1)
            hygiene_df = hygiene_df.drop(['%s_kh' % k, '%s_standort' % k, '%s_es' % k, '%s_2019' % k, '%s_2019_so' % k, '%s_2016' % k, '%s_2016_so' % k], 1)

    hygiene_df['year'] = hygiene_df['path_year']
    hygiene_df['ik-so'] = hygiene_df.apply(lambda x: '%s-%s' % (str(x['path_ik']), str(x['path_so'])), 1)
    hygiene_df = hygiene_df.reset_index()

    hygiene_df['hausnr'] = hygiene_df['hausnr'].apply(lambda x: x if not isinstance(x, float) else int(x))
    hygiene_df['address'] = hygiene_df.apply(lambda x: '%s %s' % (x['strasse'], x['hausnr']), 1)
    hygiene_df['plz'] = hygiene_df['plz'].apply(lambda x: x if pd.isnull(x) else str(int(x)).zfill(5))
    hygiene_df['ik'] = hygiene_df['ik'].apply(lambda x: x if pd.isnull(x) else str(int(x)))
    hygiene_df['so'] = hygiene_df['so'].apply(lambda x: x if pd.isnull(x) else str(int(x)))
    hygiene_df['so_alt'] = hygiene_df['so_alt'].apply(lambda x: x if pd.isnull(x) else str(x))
    #hygiene_df['ik-so'] = hygiene_df.apply(lambda x: '%s-%s' % (str(x['ik']), str(x['so'])), 1)
    hygiene_df['ik-name'] = hygiene_df.apply(lambda x: '%s-%s' % (str(x['ik']), str(x['name'])), 1)


    return hygiene_df


def get_ik_bl(x):
    return str(x)[2:4]


def assign_bundesland(hygiene_df):
    bl_mapping = dict([
        ('01', 'Schleswig-Holstein',),
        ('02', 'Hamburg',),
        ('03', 'Niedersachsen',),
        ('04', 'Bremen',),
        ('05', 'Nordrhein-Westfalen',),
        ('06', 'Hessen',),
        ('07', 'Rheinland-Pfalz',),
        ('08', 'Baden-Württemberg',),
        ('09', 'Bayern',),
        ('10', 'Saarland',),
        ('11', 'Berlin',),
        ('12', 'Brandenburg',),
        ('13', 'Mecklenburg-Vorpommern',),
        ('14', 'Sachsen',),
        ('15', 'Sachsen-Anhalt',),
        ('16', 'Thüringen',)
    ])
    hygiene_df['ik_bl'] = hygiene_df['ik'].apply(get_ik_bl)
    hygiene_df['bundesland'] = hygiene_df['ik_bl'].apply(lambda x: bl_mapping.get(x, None))
    return hygiene_df
