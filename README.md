It's a microservice which imports the data from the downloader. All data stored in a rabbitmq queue.

Every action on the database or from the microservice has an unique log identification. The following identifications are possible:
 - I1000: Data successfully from the downloader downloaded and imported to the 'import_jq'.
 - I1001: Something went wrong to import the downloaded data to the 'import_jq'.
 - I1002: Insert new dataset to table 'currency_now'.
 