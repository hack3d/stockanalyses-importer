It's a microservice which imports the data from the downloader.

Possible actions:
- Current data:
    - 1000 [init]
    - 1100 [job is in processing]
    - 1200 [data successfully imported]

Every action on the database or from the microservice has an unique log identification. The following identifications are possible:
 - I1000: Data successfully from the downloader downloaded and imported to the 'import_jq'.
 - I1001: Something went wrong to import the downloaded data to the 'import_jq'.
 - I1002: Insert new dataset to table 'currency_now'.
 
 Bitstamp:
 For the exchange bitstamp we use on table 'currency_now' the field 'currency_volume' for the vwap (volume weighted average price).