# Facebook-Ads-to-BigQuery

Script meant to be run in Google cloud functions, used to pull ads insights data fromthe Facebook API.
The script pulls the last day of available data by default.

It makes use of the Facebook Business SDK library.

The output of the Facebook API is transformed to an ND-JSON format and uploaded to a BigQuery table as is.
