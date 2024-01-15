
{{ config(materialized='table') }}
with source as (
        SELECT * FROM bigquery-public-data.fdic_banks.institutions
),
extract_1 as (
select institution_name,
state_name,
fdic_id,
active,
address,
total_assets,--	NULLABLE	INTEGER	The sum of all assets owned by the institution including cash, loans, securities, bank premises and other assets. This total does not include off-balance-sheet accounts.
change_code_1,--	NULLABLE	STRING	FDIC code used to signify a structural event relating to an institution. The definitions of the codes are available in the `bigquery-public-data.fdic_banks.change_codes`
change_code_2,--	NULLABLE	STRING	FDIC code used to signify a structural event relating to an institution. The definitions of the codes are available in the `bigquery-public-data.fdic_banks.change_codes`
city,--	NULLABLE	STRING	City in which an institution's headquarters or one of its branches is physically located.
category_code,--	NULLABLE	STRING	Numeric code which identifies the major and minor categories of an institution. Definitions of these are available in`bigquery-public-data.fdic_banks.category_code`
county_name,--	NULLABLE	STRING	County where the institution is physically located (abbreviated if the county name exceeds 16 characters).
established_date,--	NULLABLE	DATE	The date on which the institution began operations.
last_updated,--	NULLABLE	DATE	Date the data was last updated
effective_date--	NULLABLE	DATE	Effective Start Date of the data contained in this row.
from source
),
string_function as (
    select
    ascii(institution_name) as ascii_results, --Returns the ASCII code for the first character or byte in value. Returns 0 if value is empty or the ASCII code is 0 for the first character or byte.
    FORMAT('%d',total_assets) AS FORMAT_NUM_TO_CHAR, --FORMAT formats a data type expression as a string.
    char_length(state_name) as char_length_state_name, --Gets the number of characters in a STRING value.
    CHARACTER_LENGTH(state_name) as synonym_char_lenght,
    CONCAT(state_name,' : ',address) AS CONCATE_RESULT, --Concatenates one or more values into a single result. All values must be BYTES or data types that can be cast to STRING.
    CONTAINS_SUBSTR(address,'St') AS CONTAINS_SUBSTR_RESULTS, --Performs a normalized, case-insensitive search to see if a value exists as a substring in an expression. Returns TRUE if the value exists, otherwise returns FALSE.
    CONTAINS_SUBSTR('the blue house', 'Blue house') AS STANDALONE_EXAMPLE_RESULT, -- EXAMPLE FROM DOCS
    ENDS_WITH(city,'a') AS ENDS_WITH_CITY, --Takes two STRING or BYTES values. Returns TRUE if suffix is a suffix of value.
    LEFT(institution_name,5) AS LEFT_RESULTS, --Returns a STRING or BYTES value that consists of the specified number of leftmost characters or bytes from value.
    LENGTH(institution_name) AS LENGHT_RESULTS, --Returns the length of the STRING or BYTES value
    LOWER(institution_name) AS LOWER_RESULTS, --For STRING arguments, returns the original string with all alphabetic characters in lowercase.
    LTRIM(institution_name) AS LEFT_TRIM, --Identical to TRIM, but only removes leading characters.
    RTRIM(institution_name) AS RIGHT_TRIM, --Identical to TRIM, but only removes trailing characters.
    TRIM(institution_name) AS ALL_TRIM, --Takes a STRING or BYTES value to trim.
    REPEAT(institution_name,3) AS REPEAT_RESULTS, --Returns a STRING or BYTES value that consists of original_value, repeated. 
    REPLACE(institution_name,'Bank','Rank') AS REPLACE_RESULTS, --REPLACE(original_value, from_pattern, to_pattern)
    REVERSE(institution_name) AS REVERSE_RESULTS, --Returns the reverse of the input STRING or BYTES.
    RIGHT(institution_name,5) AS RIGHT_RESULTS, --Returns a STRING or BYTES value that consists of the specified number of rightmost characters or bytes from value.
    SPLIT(institution_name,',') AS SPLIT_RESULTS,-- Splits value using the delimiter argument.
    STARTS_WITH(city,'B') AS STARTS_WITH_CITY, --  Takes two STRING or BYTES values. Returns TRUE if prefix is a prefix of value.
    SUBSTR(institution_name,5) AS SUBSTR_RESULTS, --Gets a portion (substring) of the supplied STRING or BYTES value.
    UPPER(institution_name) AS UPPER_RESULTS, --For STRING arguments, returns the original string with all alphabetic characters in uppercase
    *
     from extract_1
)

SELECT * FROM string_function