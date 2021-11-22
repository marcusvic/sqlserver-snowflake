### Automatic SQL Server ingestion into Snowflake
This is a real-world project. We have an on-premise ERP with around 300 tables that must be ingested daily into Snowflake, where our data warehouse is hosted. The organization is currently doing this with Talend, and because this is the biggest Talend job, when this is done, it will be very easy to phase-out Talend.

When ready, the project will apply an objected-oriented architecture in the python scripts, ingest the content to an S3 bucket and from there load the SF tables.