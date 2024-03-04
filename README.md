# #1 - CSV Pipeline

Build an **ETL** pipeline using Apache **Spark** to process a **CSV** file containing information about customers and their orders. Create two tables: **Customers** and **Orders**, with the following schemas.

**Customers Table:**

- customer_id (int): Unique identifier for each customer.
- customer_name (string): Name of the customer.

**Orders Table:**

- order_id (int): Unique identifier for each order.
- customer_id (int): Foreign key referencing the customer_id in the Customers table.
- order_date (date): Date of the order.
- order_amount (double): Amount of the order.

**Instructions for the challenge:**

1. Create a Python module with functions and classes to encapsulate different aspects of the ETL pipeline. Use object-oriented programming principles to create modular and reusable code. [Resource.](https://www.notion.so/1-CSV-Pipeline-598d8cc93eaf4b2fb9d4eb63846b45a1?pvs=21)
2. Define a class for reading the CSV file and initializing the Spark DataFrame. This class should handle any necessary configurations for reading the file, such as specifying the schema and header options. [Resource](https://spark.apache.org/docs/latest/sql-data-sources-csv.html).
3. Implement functions for data cleaning and transformation, such as parsing dates and extracting relevant columns. Each transformation step should be encapsulated within a separate function or method for modularity and reusability. [Resource](https://www.sparkcodehub.com/spark-how-to-cleaning-and-preprocessing-data-in-spark-dataframe).
4. Write unit tests to validate the functionality of the functions and classes. Test cases should cover various scenarios, including edge cases and error conditions. [Resource](https://docs.python-guide.org/writing/tests/).
5. After preparing your DataFrame, write it using the Delta Lake format and partition it accordingly. Resources: [1](https://www.hpe.com/pt/en/what-is/delta-lake.html#:~:text=What) and [2](https://docs.delta.io/latest/best-practices.html).
6. Set up a Git repository to manage the codebase and track changes. Use branches and commits effectively to maintain a clean and organized version history. [Resource](https://product.hubspot.com/blog/git-and-github-tutorial-for-beginners).

Sample CSV Example (customers_orders.csv):

```
customer_id,customer_name,order_id,order_date,order_amount,pais
1,John Doe,101,2024-02-20,100.50,br
2,Alice Smith,102,2024-02-21,75.25,br
3,Bob Johnson,103,2024-02-22,50.75,br
4,Lisa Brown,104,2024-02-22,125.00,br
```

**Goals:**

- Have an ETL process;
- Understand how Spark works;
- Understand why Delta Lake is useful in data.

**Plus**: Write an article/text explaining the points I mentioned in the goals, that would be good to understand if you got the point or not.

**Tips**: Use either Colab or Databricks Community to run the jobs.