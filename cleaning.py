import pandas as pd
import numpy as np

import datetime

# Read the dataset
df = pd.read_csv('globalterrorismdb_0221dist.csv')

# Remove irrelevant columns for the study
to_remove = [   
                'approxdate',                                                                           # Estimated date of the incident, when it is not certain.
                'country', 'region',                                                                    # Codes for country and region.
                'summary',                                                                              # Descriptive text about the incident.
                'doubtterr',                                                                            # Flags if there is doubt about this being a terrorist attack.
                'alternative', 'alternative_txt',                                                       # If there was doubt (doubtterr flag), proposes an alternative.
                'multiple',                                                                             # If corresponds with a series of related incidents, redundant with information in 'related' column.
                'attacktype1', 'attacktype2', 'attacktype3',                                            # Codes for attack types.
                'targtype1', 'targsubtype1',                                                            # Codes for target type in target 1.
                'target1',                                                                              # Descriptive text for target 1.
                'natlty1',                                                                              # Codes for nationality of target 1.
                'targtype2', 'targsubtype2',                                                            # Applies the same last 3 lines for target 2 and 3.
                'target2',                                                          
                'natlty2',                                          
                'targtype3', 'targsubtype3',                        
                'target3',                                                          
                'natlty3',
                'gsubname', 'gsubname2', 'gsubname3',                                                   # Subdivision of group 1, 2 o 3 that has claimed the incident.
                'guncertain1', 'guncertain2', 'guncertain3',                                            # Flags if theres uncertainty about group 1, 2 or 3 taking part in the incident.
                'individual',                                                                           # Flags if the perpetrators do not correspond to a terrorist group, redudant by 'gname' column.
                'nperps',                                                                               # Number of perpetrators.
                'claimed', 'claimmode', 'claimmode_txt',                                                # Flags and codes/descriptive text about group 1 claiming the incident.
                'claim2', 'claimmode2', 'claimmode2_txt',                                               # Same for group 2 and 3.
                'claim3', 'claimmode3', 'claimmode3_txt',
                'compclaim',                                                                            # Flags if several groups claims the incident.
                'weaptype1', 'weapsubtype1',                                                            # Codes for weapon 1 type used by the perpetrators, below are codes for weapon 2, 3 and 4.
                'weaptype2', 'weapsubtype2',
                'weaptype3', 'weapsubtype3',
                'weaptype4', 'weapsubtype4',
                'weapdetail',                                                                           # Descriptive text about the weapons used by the perpetrators.
                'nkillus',  'nwoundus',                                                                 # Death and wounded toll, only taking US citizens into account.
                'propvalue', 'propcomment',                                                             # Damage to properties in USD (present in only few cases), and descriptive text about the damage.
                'nhostkidus',                                                                           # Number of hostages taken, only taking US citizens into account.
                'divert',                                                                               # Country that the kidnappers diverted after the resolution of the incident.
                'kidhijcountry',                                                                        # Country where the hostage incident was resolved.
                'ransom', 'ransomamt', 'ransomamtus', 'ransompaid', 'ransompaidus', 'ransomnote',       # Information about the ransom involving the hostage incident.
                'hostkidoutcome',                                                                       # Code for the outcome of the hostages.
                'addnotes',                                                                             # Descriptive text for additional notes about the incident.
                'scite1', 'scite2', 'scite3', 'dbsource',                                               # Information sources for the incident.
                'INT_MISC', 'INT_ANY',                                                                  # Flags if the incidents corresponds to an International misc. or any International type.
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

# Add a new column: perpcapture that states the amount of captured perpetrators
# where:    na. unknown
#            0. no perpetrator captured
#            1. at least one perpetrator captured
perpcapture = np.where(df.nperpcap == 0, 0, np.nan)
perpcapture = np.where(df.nperpcap > 0, 1, perpcapture)
df['perpcapture'] = perpcapture
df = df.loc[:, df.columns!='nperpcap']

# Merge property, propextent and propextent_txt columns into propdamage (interval for property damage cost) column
# where:     na.   unknown
#             0.    no damage
#             1.    minor           (cost < 1 million)
#             2.    major           (1 million <= cost < 1 billion)
#             3.    catastrophic    (1 billion < cost)
df['propdamage'] = np.where(df.property == 0, 0, np.nan)
df['propdamage'] = np.where(df.propextent == 4, np.nan, df.propdamage)
df['propdamage'] = np.where(df.propextent == 3, 1, df.propdamage)
df['propdamage'] = np.where(df.propextent == 2, 2, df.propdamage)
df['propdamage'] = np.where(df.propextent == 1, 3, df.propdamage)
df = df.loc[:, ~df.columns.isin(['property', 'propextent', 'propextent_txt'])]

# Reset indexes and delete the newly generated index column, containing the old indexes
df = df.reset_index()
df = df.loc[:, df.columns!='index']

# Ugly code to remove deleted references from related column
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

# Change blank values in related for nan
df.related = np.where(df.related == '', np.nan, df.related)

# Replace unknown values for n/a
columns_with_unknown_values = {
                                -9:         [
                                                'vicinity',
                                                'ishostkid', 'nhours',
                                                'INT_LOG', 'INT_IDEO'
                                            ],
                                -99:        [
                                                'nhostkid', 'nhours','ndays', 'nreleased'

                                            ],
                                'Unknown':  [
                                                'attacktype1_txt', 'attacktype2_txt', 'attacktype3_txt',
                                                'targtype1_txt', 'targtype2_txt', 'targtype3_txt',
                                                'gname',
                                                'motive',
                                                'weaptype1_txt', 'weaptype2_txt', 'weaptype3_txt', 'weaptype4_txt',
                                                'hostkidoutcome_txt'
                                            ]
}

for value in columns_with_unknown_values.keys():
    for column in columns_with_unknown_values[value]:
        df[column] = np.where(df[column] == value, np.nan, df[column])

# Reorder columns
column_order = [
                    'eventid',
                    'timestamp', 'end_date', 'duration',
                    'country_txt', 'region_txt', 'provstate', 'city', 'location',
                    'latitude', 'longitude', 'specificity', 'vicinity',
                    'crit1', 'crit2',
                    'success', 'suicide',
                    'attacktype1_txt', 'attacktype2_txt', 'attacktype3_txt',
                    'targtype1_txt', 'targsubtype1_txt', 'corp1', 'natlty1_txt',
                    'targtype2_txt', 'targsubtype2_txt', 'corp2', 'natlty2_txt',
                    'targtype3_txt', 'targsubtype3_txt', 'corp3', 'natlty3_txt',
                    'gname', 'gname2', 'gname3',
                    'motive',
                    'weaptype1_txt', 'weapsubtype1_txt',
                    'weaptype2_txt', 'weapsubtype2_txt',
                    'weaptype3_txt', 'weapsubtype3_txt',
                    'weaptype4_txt', 'weapsubtype4_txt',
                    'nkill', 'nkillter', 'nwound', 'nwoundte', 'perpcapture',
                    'ishostkid', 'nhostkid', 'nhours', 'ndays', 'hostkidoutcome_txt', 'nreleased',
                    'propdamage',
                    'INT_LOG', 'INT_IDEO',
                    'related', 'n_related'
               ]

df = df.reindex(columns=column_order)

# Save the cleaned dataframe
df.to_csv('out.csv', index=False, na_rep=np.nan)
