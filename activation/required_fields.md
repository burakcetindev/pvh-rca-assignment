# Required Fields for Google Ads Conversion Upload

The following fields must be provided when uploading a conversion to Google Ads API:

- **gclid**: Google Click Identifier (string, required)  
  Unique identifier for the click that led to the conversion; used for accurate ad attribution.

- **conversion_action**: The conversion action name/id (string, required)  
  Specifies the type of conversion being recorded, e.g., "ORDER_COMPLETED".

- **conversion_date_time**: Timestamp of the conversion event in ISO8601 format (string, required)  
  Represents the exact time the conversion occurred.

- **conversion_value**: The monetary value of the conversion (float, required)  
  The revenue or value associated with this conversion for reporting purposes.

- **currency_code**: The currency code for the conversion value, e.g., USD (string, required)  
  Specifies the currency of `conversion_value` for accurate financial reporting.