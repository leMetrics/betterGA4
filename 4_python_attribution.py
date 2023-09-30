import pandas as pd
import numpy as np
import csv
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq

GBQ_PROJECT = "<your-project>" # Google Cloud Project-ID
GBQ_KEYPATH = "./<your-keyfile>.json" # Path to Service Account JSON keyfile
GBQ_SQL_PATHS = ".measurecamp_attribution.sql" # Path to SQL file -> querying the conversions paths (see tutorial)

GBQ_TABLE_CHANNELS = "ledata.GA4CChannels.conversions_attr_paths" # Table name for storing attributed conversions on journey level
GBQ_TABLE_CHANNELS_GROUPED = "ledata.GA4CChannels..conversions_attr_channels" # Table name for storing conversions grouped on channel and date level


# Create BigQuey Client
credentials = service_account.Credentials.from_service_account_file(
    GBQ_KEYPATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Open the .sql file
with open(GBQ_SQL_PATHS,"r") as f:
    sql = f.read()

# Query BigQuery and convert to dataframe
df_conversion_paths = pd.read_gbq(sql, credentials=credentials)

# View information
df_conversion_paths.info()

# Create and configure the attribution lib instance
attributions = MAM(df_conversion_paths,
    group_channels=False,
    channels_colname="path_channels",
    time_till_conv_colname="path_timestamps",
    group_channels_by_id_list=["journey_id"]
)

# Models and model settings + running the models
models = {
    "attr_first_click": attributions.attribution_first_click(),
    "attr_last_click": attributions.attribution_last_click(),
    "attr_last_non_direct_click": attributions.attribution_last_click_non(but_not_this_channel='direct'),
    "attr_position": attributions.attribution_position_based(list_positions_first_middle_last=[0.3, 0.3, 0.4]),
    "attr_time_decay": attributions.attribution_time_decay(decay_over_time=0.6, frequency=7),
    "attr_linear": attributions.attribution_linear(),
    "attr_markov": attributions.attribution_markov(transition_to_same_state=False),
    #"attr_shapley": attributions.attribution_shapley(size=4, order=True, values_col='conversions'), -> Not available on path level
}

# View results grouped by channel for complete conversion period
print(attributions.group_by_channels_models)

# Join models into one dataframe
models_list = [df_conversion_paths]

for model in models:
    attr = pd.DataFrame(models[model][0])[model]
    attr = attr.rename(columns={attr.columns[0]: model})
    models_list.append(attr)

df_attributions = pd.concat(models_list, axis=1)
df_attributions["path_channels"] = df_attributions["path_channels"].apply(lambda x: x.split(" > "))
df_attributions["path_timestamps"] = df_attributions["path_timestamps"].apply
