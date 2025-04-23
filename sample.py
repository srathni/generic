AE_DERIVATION_LOOKUP - Event Condition Configuration
This design enables business teams to define the conditions that trigger events (e.g., LATE_FEE, ACCOUNT_CHANGE, DELINQUENT) through key-value pairs, without directly modifying the SQL queries. Each condition is entered as a separate row in the AE_DERIVATION_LOOKUP table, and during execution, the ETL framework dynamically pivots the rows into a single row for use in SQL queries.

Table Structure: AE_DERIVATION_LOOKUP


Column Name	Description
SELECTION_KEY_1	A key that represents the condition column (e.g., a, b, c)
SELECTION_KEY_VALUE_1	The value(s) associated with the condition column (e.g., 2, 3, (4,5))
EVENT_TYPE	The type of event being generated (e.g., LATE_FEE, ACCOUNT_CHANGE)
ACTIVE_FLAG	Indicates whether the condition is currently active or not
Process Flow:

Configuration:
Business users interact with the AE_DERIVATION_LOOKUP table to define conditions. For example, if the SQL is SELECT ... FROM table1 WHERE a=2 AND b=3 AND c IN (4,5), the business team enters the following rows:
SELECTION_KEY_1 = "a", SELECTION_KEY_VALUE_1 = "2"
SELECTION_KEY_1 = "b", SELECTION_KEY_VALUE_1 = "3"
SELECTION_KEY_1 = "c", SELECTION_KEY_VALUE_1 = "(4,5)"
EVENT_TYPE = "LATE_FEE" for all rows.
SQL Generation:
The SQL query (e.g., SELECT ... FROM table1 WHERE a=2 AND b=3 AND c IN (4,5)) remains rigid and unchanged in structure. The ETL framework dynamically pivots the rows from AE_DERIVATION_LOOKUP into a single row for each condition and integrates the results into the SQL query.
Pivoting Logic:
At runtime, the ETL framework uses a pivot function to convert the multiple rows from AE_DERIVATION_LOOKUP into a single row. This single row is then used in the SQL to replace the hardcoded values.
For example, the ETL framework generates a dynamic SQL that looks like this:

SELECT ...
FROM table1
WHERE a = (SELECT SELECTION_KEY_VALUE_1 FROM AE_DERIVATION_LOOKUP WHERE SELECTION_KEY_1 = 'a' AND ACTIVE_FLAG = 1)
AND b = (SELECT SELECTION_KEY_VALUE_1 FROM AE_DERIVATION_LOOKUP WHERE SELECTION_KEY_1 = 'b' AND ACTIVE_FLAG = 1)
AND c IN (SELECT SELECTION_KEY_VALUE_1 FROM AE_DERIVATION_LOOKUP WHERE SELECTION_KEY_1 = 'c' AND ACTIVE_FLAG = 1)
This dynamic query now incorporates the conditions from the AE_DERIVATION_LOOKUP table, and the values for a, b, and c are flexible and can be adjusted without altering the SQL itself.
Event Detection:
When the ETL job runs, the SQL query is executed as usual. If the conditions (e.g., a=2, b=3, and c IN (4,5)) are met, the corresponding event (e.g., LATE_FEE) is triggered and processed accordingly.
Benefits:
Flexibility: Business users can configure event conditions without modifying SQL, providing them with autonomy while maintaining query integrity.
Security: The original SQL logic remains unchanged, which reduces the risk of SQL injection or accidental changes.
Separation of Concerns: SQL queries and event configuration are separated, making the system easier to manage and extend.
Auditability: Changes to event conditions are tracked in the AE_DERIVATION_LOOKUP table, ensuring a transparent history of modifications.
How the Pivot Works in Practice:
To elaborate on the pivoting:

In your SQL generation process, the framework will use SQL queries to pivot the multiple rows from AE_DERIVATION_LOOKUP into a format that can be directly integrated into the main SQL query.
For example, given the following rows in the AE_DERIVATION_LOOKUP table:


SELECTION_KEY_1	SELECTION_KEY_VALUE_1	EVENT_TYPE	ACTIVE_FLAG
a	2	LATE_FEE	1
b	3	LATE_FEE	1
c	(4,5)	LATE_FEE	1
The pivoting logic would transform them into:

SELECT
    (SELECT SELECTION_KEY_VALUE_1 FROM AE_DERIVATION_LOOKUP WHERE SELECTION_KEY_1 = 'a' AND ACTIVE_FLAG = 1) AS a_val,
    (SELECT SELECTION_KEY_VALUE_1 FROM AE_DERIVATION_LOOKUP WHERE SELECTION_KEY_1 = 'b' AND ACTIVE_FLAG = 1) AS b_val,
    (SELECT SELECTION_KEY_VALUE_1 FROM AE_DERIVATION_LOOKUP WHERE SELECTION_KEY_1 = 'c' AND ACTIVE_FLAG = 1) AS c_val
Then, these a_val, b_val, and c_val would be integrated into the final SQL where needed.

This approach ensures the flexibility and configurability you're aiming for while maintaining the integrity of the SQL. The users can easily modify conditions in the AE_DERIVATION_LOOKUP table without the need for direct SQL changes, and the framework handles the dynamic transformation and pivoting.
