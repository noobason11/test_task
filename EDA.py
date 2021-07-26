from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as pl



project = 'clever-axe-251109'
dataset_id = 'google_analytics_sample'
table_id = 'ga_sessions_20170801'
credentials = service_account.Credentials.from_service_account_file('db_reporting.json')
client_bq = bigquery.Client(project, credentials=credentials)


def get_data(client):
    df = client.query(
        """
        SELECT
  fullVisitorId,
  channelGrouping,
  geoNetwork.country,
  geoNetwork.region,
  device.deviceCategory,
  ROUND(SUM(totals.totalTransactionRevenue/1000000), 2) AS Revenue,
  SUM( totals.visits ) AS visits
FROM
  bigquery-public-data.google_analytics_sample.ga_sessions_20170801
WHERE
  fullVisitorId IS NOT NULL
  AND totals.totalTransactionRevenue IS NOT NULL
GROUP BY
  geoNetwork.country,
  geoNetwork.region,
  channelGrouping,
  device.deviceCategory,
  fullVisitorId
        """
    ).result().to_dataframe()
    return df



def buildPlots(df):
    df_visits = df.groupby(by="region")["visits","Revenue"].sum()
    df_visits.plot(kind='bar',subplots=True, figsize=(11, 6))
    pl.show()
    df_devices = df.groupby(by="deviceCategory")["visits", "Revenue"].sum()
    df_devices.plot.pie(subplots=True, figsize=(11, 6))
    pl.show()
    df_channels = df.groupby(by="channelGrouping")["visits", "Revenue"].sum()
    df_channels.plot.pie(subplots=True, figsize=(11, 6))
    pl.show()

if __name__ == '__main__':
    df = get_data(client_bq)
    if len(df) != 0:
        buildPlots(df)