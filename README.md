# PySpark Ecommerce Data Pipeline

This project demonstrates a simple data engineering pipeline using PySpark running inside Docker.

The pipeline reads a comma-separated ecommerce dataset, converts it into a structured DataFrame, performs basic processing, and saves the formatted output.

## Technologies Used

- Apache Spark
- PySpark
- Python
- Docker

## Dataset

The dataset contains ecommerce order data including:

- order_id
- customer_id
- product_id
- product_category
- order_date
- quantity
- price
- payment_method
- country
- customer_email

## Project Workflow

CSV Dataset
      ↓
PySpark reads CSV
      ↓
Convert to DataFrame
      ↓
Data Processing
      ↓
Formatted Output

## How to Run

Start the Spark container:

docker-compose up -d

Run the PySpark script:

sh run.sh sample.py

## Example Output

The script reads the CSV dataset and converts it into a Spark DataFrame.

It also saves the processed output into the output directory.

## Future Improvements

- Data cleaning for missing values
- Sales aggregation analysis
- Product category insights
- Integration with a data warehouse
