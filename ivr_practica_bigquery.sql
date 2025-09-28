
-- 3. CREAR TABLA DE ivr_detail.
CREATE OR REPLACE TABLE keepcoding.ivr_detail
(
  calls_ivr_id FLOAT64,
  calls_phone_number string,
  calls_ivr_result string,
  calls_vdn_label string,
  calls_start_date TIMESTAMP,
  calls_start_date_id INT64,
  calls_end_date TIMESTAMP,
  calls_end_date_id INT64,
  calls_total_duration INT64,
  calls_customer_segment string,
  calls_ivr_language string,
  calls_steps_module INT64,
  calls_module_aggregation string,
  module_sequece INT64,
  module_name string,
  module_duration INT64,
  module_result string,
  step_sequence INT64,
  step_name string,
  step_result string,
  step_description_error string,
  document_type string,
  document_identification string,
  customer_phone string,
  billing_account_id string
);

INSERT INTO keepcoding.ivr_detail (
calls_ivr_id,
calls_phone_number,
calls_ivr_result,
calls_vdn_label,
calls_start_date,
calls_start_date_id,
calls_end_date,
calls_end_date_id,
calls_total_duration,
calls_customer_segment,
calls_ivr_language,
calls_steps_module,
calls_module_aggregation,
module_sequece,
module_name,
module_duration,
module_result,
step_sequence,
step_name,
step_result,
step_description_error,
document_type,
document_identification,
customer_phone,
billing_account_id
)
SELECT
ca.ivr_id,
ca.phone_number,
ca.ivr_result,
ca.vdn_label,
ca.start_date,
CAST(FORMAT_DATE('%Y%m%d', DATE(ca.start_date)) AS INT64),
ca.end_date,
CAST(FORMAT_DATE('%Y%m%d', DATE(ca.end_date)) AS INT64),
ca.total_duration,
ca.customer_segment,
ca.ivr_language,
ca.steps_module,
ca.module_aggregation,
mo.module_sequece,
mo.module_name,
mo.module_duration,
mo.module_result,
st.step_sequence,
st.step_name,
st.step_result,
st.step_description_error,
st.document_type,
st.document_identification,
st.customer_phone,
st.billing_account_id

FROM keepcoding.ivr_calls ca
LEFT JOIN keepcoding.ivr_modules mo
on ca.ivr_id = mo.ivr_id
LEFT JOIN keepcoding.ivr_steps st
on mo.ivr_id = st.ivr_id
AND mo.module_sequece = st.module_sequece;

-- 4. Generar el campo vdn_aggregation
SELECT DISTINCT calls_ivr_id,
 CASE WHEN STARTS_WITH(calls_vdn_label, 'ATC') THEN 'FRONT'
WHEN STARTS_WITH(calls_vdn_label, 'TECH') THEN 'TECH'
WHEN calls_vdn_label='ABSORPTION' THEN 'ABSORPTION'
ELSE 'RESTO'
END AS vdn_aggregation
FROM `keepcoding.ivr_detail`
ORDER BY calls_ivr_id
;


--5. Generar los campos document_type y document_identification
SELECT 
  DISTINCT calls_ivr_id,
  document_type,
  document_identification
  FROM `keepcoding.ivr_detail`
  WHERE document_type != 'UNKNOWN'
  AND document_type IS NOT NULL   --aqui no es necesario poner IS NOT NULL porque ya verifiqué que no hay campos nulos pero por ser estricto lo pongo y para evitar futuros problemas cuando se añada mas datos.
  AND document_identification != 'UNKNOWN'
  AND document_identification IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY step_sequence) = 1 --aqui tengo que hacer un casting porque en bigquery no me deja que use particion con tipo de datos FLOAT
ORDER BY calls_ivr_id;

--6. Generar el campo customer_phone

SELECT 
  DISTINCT calls_ivr_id,
  customer_phone
  FROM `keepcoding.ivr_detail`
  WHERE customer_phone!= 'UNKNOWN'
  AND customer_phone IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY step_sequence) = 1
ORDER BY calls_ivr_id;

--7. Generar el campo billing_account_id
SELECT 
  DISTINCT calls_ivr_id,
  billing_account_id
  FROM keepcoding.ivr_detail
  WHERE billing_account_id!= 'UNKNOWN'
  AND billing_account_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY step_sequence) = 1
ORDER BY calls_ivr_id;

--8. Generar el campo masiva_lg
SELECT
  DISTINCT calls_ivr_id,
  CASE WHEN CONTAINS_SUBSTR(calls_module_aggregation, 'AVERIA_MASIVA') THEN 1
  ELSE 0
  END AS masiva_lg
FROM `keepcoding.ivr_detail`
ORDER BY calls_ivr_id;

--9. Generar el campo info_by_phone_lg

SELECT
  calls_ivr_id,
  MAX(CASE WHEN step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_result='OK' THEN 1
  ELSE 0
  END) AS  info_by_phone_lg
FROM  `keepcoding.ivr_detail`
GROUP BY calls_ivr_id
ORDER BY calls_ivr_id
;

--10. Generar el campo info_by_dni_lg
SELECT
  calls_ivr_id,
  MAX(CASE WHEN step_name = 'CUSTOMERINFOBYDNI.TX' AND step_result='OK' THEN 1
  ELSE 0
  END) AS  info_by_dni_lg
FROM `keepcoding.ivr_detail`
GROUP BY calls_ivr_id
ORDER BY calls_ivr_id;

--11. Generar los campos repeated_phone_24H, cause_recall_phone_24H
--usar la fecha calls_start_date directamente y la funcion TIMESTAMP_DIFF. Aunque he probado con DATETIME_DIFF y tb funciona, pero como mi tipo de datos de calls_start_date es TIMESTAMP, mejor uso timestamp_diff
WITH cte_calls_base AS (
  SELECT DISTINCT
    calls_ivr_id,
    calls_phone_number,
    calls_start_date
  FROM `keepcoding.ivr_detail`
),
cte_calls AS (
SELECT
  calls_ivr_id,
  calls_start_date,
  LEAD(calls_start_date) OVER(PARTITION BY calls_phone_number ORDER BY calls_start_date) AS next_phone_call,
  LAG(calls_start_date) OVER(PARTITION BY calls_phone_number ORDER BY calls_start_date) AS last_phone_call
FROM cte_calls_base
)
SELECT DISTINCT calls_ivr_id,
CASE WHEN last_phone_call IS NOT NULL AND TIMESTAMP_DIFF(calls_start_date, last_phone_call, HOUR)<=24 THEN 1
ELSE 0 END AS repeated_phone_24H,
CASE WHEN next_phone_call IS NOT NULL AND TIMESTAMP_DIFF(next_phone_call, calls_start_date, HOUR)<=24 THEN 1
ELSE 0 END AS cause_recall_phone_24H
FROM cte_calls
ORDER BY calls_ivr_id;

--12. CREAR TABLA DE ivr_summary (Para nota)
CREATE OR REPLACE TABLE  keepcoding.ivr_summary AS
WITH vnd_agg AS(
SELECT DISTINCT calls_ivr_id,
 CASE WHEN STARTS_WITH(calls_vdn_label, 'ATC') THEN 'FRONT'
WHEN STARTS_WITH(calls_vdn_label, 'TECH') THEN 'TECH'
WHEN calls_vdn_label='ABSORPTION' THEN 'ABSORPTION'
ELSE 'RESTO'
END AS vdn_aggregation
FROM `keepcoding.ivr_detail`
ORDER BY calls_ivr_id),
document_type_and_id AS(
  SELECT 
  DISTINCT calls_ivr_id,
  document_type,
  document_identification
  FROM `keepcoding.ivr_detail`
  WHERE document_type != 'UNKNOWN'
  AND document_identification != 'UNKNOWN'
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY step_sequence) = 1
ORDER BY calls_ivr_id
),
customer_phone AS (
  SELECT 
  DISTINCT calls_ivr_id,
  customer_phone
  FROM `keepcoding.ivr_detail`
  WHERE customer_phone!= 'UNKNOWN'
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY step_sequence) = 1
ORDER BY calls_ivr_id
),
billing_account_id AS (
  SELECT 
  DISTINCT calls_ivr_id,
  billing_account_id
  FROM keepcoding.ivr_detail
  WHERE billing_account_id!= 'UNKNOWN'
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY step_sequence) = 1
ORDER BY calls_ivr_id
),
masiva_lg AS (
  SELECT
  DISTINCT calls_ivr_id,
  CASE WHEN CONTAINS_SUBSTR(calls_module_aggregation, 'AVERIA_MASIVA') THEN 1
  ELSE 0
  END AS masiva_lg
FROM `keepcoding.ivr_detail`
ORDER BY calls_ivr_id
),
info_by_phone_lg AS (
  SELECT
  DISTINCT calls_ivr_id,
  MAX(CASE WHEN step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_result='OK' THEN 1
  ELSE 0
  END) AS  info_by_phone_lg
FROM `keepcoding.ivr_detail`
GROUP BY calls_ivr_id
ORDER BY calls_ivr_id
),
info_by_dni_lg AS (
  SELECT
  DISTINCT calls_ivr_id,
  MAX(CASE WHEN step_name = 'CUSTOMERINFOBYDNI.TX' AND step_result='OK' THEN 1
  ELSE 0
  END) AS  info_by_dni_lg
FROM `keepcoding.ivr_detail`
GROUP BY calls_ivr_id
ORDER BY calls_ivr_id
),
repeated_cause_recall_phone_24H AS (
  WITH cte_calls_base AS (
  SELECT DISTINCT
    calls_ivr_id,
    calls_phone_number,
    calls_start_date
  FROM `keepcoding.ivr_detail`
),
cte_calls AS (
SELECT
  calls_ivr_id,
  calls_start_date,
  LEAD(calls_start_date) OVER(PARTITION BY calls_phone_number ORDER BY calls_start_date) AS next_phone_call,
  LAG(calls_start_date) OVER(PARTITION BY calls_phone_number ORDER BY calls_start_date) AS last_phone_call
FROM cte_calls_base
)
SELECT DISTINCT calls_ivr_id,
CASE WHEN last_phone_call IS NOT NULL AND TIMESTAMP_DIFF(calls_start_date, last_phone_call, HOUR)<=24 THEN 1
ELSE 0 END AS repeated_phone_24H,
CASE WHEN next_phone_call IS NOT NULL AND TIMESTAMP_DIFF(next_phone_call, calls_start_date, HOUR)<=24 THEN 1
ELSE 0 END AS cause_recall_phone_24H
FROM cte_calls
ORDER BY calls_ivr_id
)
SELECT DISTINCT de.calls_ivr_id AS ivr_id,
  de.calls_phone_number AS phone_number,
  de.calls_ivr_result AS ivr_result,
  vnd.vdn_aggregation AS vdn_aggregation,
  de.calls_start_date AS start_date,
  de.calls_end_date AS end_date,
  de.calls_total_duration AS total_duration,
  de.calls_customer_segment AS customer_segment,
  de.calls_ivr_language AS ivr_language,
  de.calls_steps_module AS steps_module,
  de.calls_module_aggregation AS module_aggregation,
  dti.document_type AS document_type,
  dti.document_identification AS document_identification,
  cp.customer_phone AS customer_phone,
  bai.billing_account_id AS billing_account_id,
  ml.masiva_lg AS masiva_lg,
  ibpl.info_by_phone_lg AS info_by_phone_lg,
  ibdl.info_by_dni_lg AS info_by_dni_lg,
  rcrp.repeated_phone_24H AS repeated_phone_24H,
  rcrp.cause_recall_phone_24H AS cause_recall_phone_24H
  FROM keepcoding.ivr_detail de
  left join vnd_agg vnd
  ON de.calls_ivr_id = vnd.calls_ivr_id
  left join document_type_and_id dti
  ON de.calls_ivr_id = dti.calls_ivr_id
  left join customer_phone cp
  ON de.calls_ivr_id = cp.calls_ivr_id
  left join billing_account_id bai
  ON de.calls_ivr_id = bai.calls_ivr_id
  left join masiva_lg ml
  ON de.calls_ivr_id = ml.calls_ivr_id
  left join info_by_phone_lg ibpl
  ON de.calls_ivr_id = ibpl.calls_ivr_id
  left join info_by_dni_lg ibdl
  ON de.calls_ivr_id = ibdl.calls_ivr_id
  left join repeated_cause_recall_phone_24H rcrp
  ON de.calls_ivr_id = rcrp.calls_ivr_id
  ORDER BY ivr_id;


--13. CREAR FUNCIÓN DE LIMPIEZA DE ENTEROS
--forma 1
CREATE OR REPLACE FUNCTION keepcoding.clean_integer(input_value INT64) RETURNS INT64 AS
(IF(input_value IS NULL,-999999,input_value));
--forma 2
CREATE OR REPLACE FUNCTION keepcoding.clean_integer(input_value INT64) RETURNS INT64 AS
(COALESCE(input_value,-999999));








