import pandas as pd
import numpy as np

import datetime

# Read the dataset
df = pd.read_csv('globalterrorismdb_0221dist.csv')

# Remove irrelevant columns for the study
to_remove = [   
                'approxdate',                                                                           # Fecha estimada de ataque cuando no hay certeza
                'country', 'region',                                                                    # Codigos de pais y region
                'summary',                                                                              # Texto descriptivo del incidente
                'doubtterr',                                                                            # Si existe duda de que sean incidentes terroristas
                'alternative', 'alternative_txt',                                                       # Si existe duda, propone alternativas
                'multiple',                                                                             # Si corresponde a una serie de incidentes multiples, rendudante con info en atributo 'related'
                'attacktype1', 'attacktype2', 'attacktype3',                                            # Codigos de tipos de ataque
                'targtype1', 'targsubtype1',                                                            # Codigos de tipo de objetivo 1
                'target1',                                                                              # Texto descriptivo del objetivo 1
                'natlty1',                                                                              # Codigo de nacionalidad del objetivo 1
                'targtype2', 'targsubtype2',                        
                'target2',                                                          
                'natlty2',                                          
                'targtype3', 'targsubtype3',                        
                'target3',                                                          
                'natlty3',
                'gsubname', 'gsubname2', 'gsubname3',                                                   # Sub division del grupo 1, 2 o 3 que se atribuyo el incidente
                'guncertain1', 'guncertain2', 'guncertain3',                                            # Incertezas que el grupo 1, 2 o 3 hayan realizado el incidente
                'individual',                                                                           # Indica si los perpertradores no corresponden a un grupo, redudante por gname
                'nperps',                                                                               # Numero de perpetradores
                'claimed', 'claimmode', 'claimmode_txt',                                                # Info sobre si un grupo se adjudico la realizacion del incidente
                'claim2', 'claimmode2', 'claimmode2_txt',
                'claim3', 'claimmode3', 'claimmode3_txt',
                'compclaim',                                                                            # Si varios grupos se adjudican la realizacion del incidente a la vez
                'weaptype1', 'weapsubtype1',                                                            # Codigos de tipo de arma utilizada por los perpetradores
                'weaptype2', 'weapsubtype2',                                                            # Codigos de tipo de arma utilizada por los perpetradores
                'weaptype3', 'weapsubtype3',
                'weaptype4', 'weapsubtype4',
                'weapdetail',                                                                           # Texto descriptivo sobre detalles de las armas utilizada
                'nkillus',  'nwoundus',                                                                 # Cuenta de muertos y heridos de solo ciudadanos estadounidenses
                'propvalue', 'propcomment',                                                             # Valor de daño a la propiedad en USD y comentario descriptivo del daño
                'nhostkidus',                                                                           # Numero de rehenes tomados, contando solo ciudadanos estadounidenses
                'divert',                                                                               # Pais al que escaparon los secuestradores
                'kidhijcountry',                                                                        # Pais en donde se resolvió el incidente con rehenes
                'ransom', 'ransomamt', 'ransomamtus', 'ransompaid', 'ransompaidus', 'ransomnote',       # Informacion sobre el rescate pedido por los secuestradores
                'hostkidoutcome',                                                                       # Codigo para el desenlace de los rehenes
                'addnotes',                                                                             # Texto descriptivo, notas adicionales del incidente
                'scite1', 'scite2', 'scite3', 'dbsource',                                               # Fuentes de informacion sobre el incidente
                'INT_MISC', 'INT_ANY',                                                                  # Flags si el incidente cae en las categorias internacional miscelaneo, o internacional cualquiera.
            ]

df = df.loc[:, ~df.columns.isin(to_remove)]


# Remove incidents: 1.  Ocurred before 1998
#                   2.  With day/month set as 0
df = df[(df.iyear >= 1998) & (df.iday > 0) & (df.imonth > 0)]

# Remove incidents ocurred within context of war
df = df[df.crit3 != 0]
df = df.loc[:, df.columns!='crit3']

# Merge iyear, imonth, iday column into a timestamp column within a timestamp format
df['timestamp'] = df[['iyear', 'imonth', 'iday']].apply(lambda s: datetime.datetime(*s), axis=1)
df = df.loc[:, ~df.columns.isin(['iyear', 'imonth', 'iday'])]

# Merge extended, resolution columns into end_date and duration (days) column
end_date  = pd.to_datetime(df['resolution'], format='%Y-%m-%d')
df['end_date'] = np.where(pd.isnull(df['resolution']), df['timestamp'], end_date)
df['duration'] = (df['end_date'] - df['timestamp']).apply(lambda s: s.days + 1)
df = df.loc[:, ~df.columns.isin(['extended', 'resolution'])]

# Filter out incidents where duration has a non positive value
df = df[df.duration > 0]

# Merge property, propextent and propextent_txt columns into propdamage (interval for property damage cost) column
# where:    # -1.   unknown
            # 0.    no damage
            # 1.    minor           (cost < 1 million)
            # 2.    major           (1 million <= cost < 1 billion)
            # 3.    catastrophic    (1 billion < cost)
df['propdamage'] = np.where(df.property == 0, 0, -1)
df['propdamage'] = np.where(df.propextent == 4, -1, df.propdamage)
df['propdamage'] = np.where(df.propextent == 3, 1, df.propdamage)
df['propdamage'] = np.where(df.propextent == 2, 2, df.propdamage)
df['propdamage'] = np.where(df.propextent == 1, 3, df.propdamage)
df = df.loc[:, ~df.columns.isin(['property', 'propextent', 'propextent_txt'])]

# Reset indexes and delete the newly generated index column, containing the old indexes
df = df.reset_index()
df = df.loc[:, df.columns!='index']

# Remove deleted references from related column
for i in range(len(df)):
    related_incidents_str = df.at[i, 'related']
    if pd.isna(related_incidents_str):
        df.at[i, 'related'] = ''
    else:
        related_incidents_initial = [int(event_id) for event_id in related_incidents_str.replace(' ', '').split(',')]
        
        related_incidents_final = []
        for event_id in related_incidents_initial:
            if event_id in df.eventid.values:
                related_incidents_final.append(event_id)

        df.at[i, 'related'] = ",".join([str(i) for i in related_incidents_final])

# Add a n_related column, displaying how many related incidents for each incident there are
n_related = np.where(df.related != '', df.related.apply(lambda s: len(s.split(','))), 0)
df['n_related'] = n_related

# Reorder columns
df.insert(1, 'timestamp', df.pop('timestamp'))
df.insert(2, 'end_date', df.pop('end_date'))
df.insert(3, 'duration', df.pop('duration'))
df.insert(55, 'propdamage', df.pop('propdamage'))

# Save the cleaned dataframe
df.to_csv('out.csv', index=False)
