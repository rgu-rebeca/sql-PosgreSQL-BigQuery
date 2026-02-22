# 📞 IVR Data Warehouse – Advanced SQL Practice
## 📌 Descripción del Proyecto

Este proyecto consiste en el diseño e implementación de un modelo de datos para una IVR (Interactive Voice Response) de atención al cliente, desarrollando tanto el modelo relacional como las transformaciones necesarias para construir una capa analítica tipo Data Warehouse.

La práctica incluye:

- Modelado entidad-relación
- Creación de base de datos en PostgreSQL
- Construcción de tabla detallada (ivr_detail)
- Generación de campos derivados y flags analíticos
- Construcción de tabla resumen (ivr_summary)
- Creación de función de limpieza (clean_integer)

El objetivo principal es aplicar conceptos avanzados de SQL, modelado de datos y transformación analítica.

## 🏗️ Arquitectura del Proyecto

El sistema se basa en tres tablas fuente:

- ivr_calls → Información general de la llamada

- ivr_modules → Módulos por los que pasa la llamada

- ivr_steps → Pasos dentro de cada módulo

Tal como se describe en el enunciado ():

ivr_modules se relaciona con ivr_calls mediante ivr_id

ivr_steps se relaciona con ivr_modules mediante ivr_id + module_sequence

## 🧱 1️⃣ Modelo Entidad–Relación

Se diseñó un modelo relacional que permite:

- Representar la jerarquía llamada → módulo → step
- Mantener integridad referencial
- Permitir explotación analítica posterior

El modelo respeta:

- PKs y FKs correctamente definidas
- Cardinalidades 1:N entre llamadas y módulos
- Cardinalidades 1:N entre módulos y steps

## 🗄️ 2️⃣ Creación de Base de Datos

Se desarrolló un script SQL compatible con PostgreSQL que:

- Crea todas las tablas

- Define restricciones

- Implementa claves primarias y foráneas

- Garantiza integridad referencial

## 📊 3️⃣ Tabla ivr_detail

Se construyó la tabla ivr_detail, que representa el nivel más granular del modelo analítico.

Incluye:

**📞 Datos de llamada**

calls_ivr_id

calls_phone_number

calls_ivr_result

calls_vdn_label

calls_start_date

calls_end_date

calls_total_duration

calls_customer_segment

calls_ivr_language

**📅 Campos calculados de fecha**

calls_start_date_id → formato yyyymmdd

calls_end_date_id → formato yyyymmdd

Ejemplo:

2023-01-01 → 20230101

📦 Información de módulos y pasos

module_sequence

module_name

module_duration

module_result

step_sequence

step_name

step_result

step_description_error

**👤 Identificación del cliente**

document_type

document_identification

customer_phone

billing_account_id

## 🧮 4️⃣ Campos Derivados
**🔹 vdn_aggregation**

Generalización de vdn_label según lógica definida ():

Condición	Resultado
Empieza por ATC	FRONT
Empieza por TECH	TECH
Es ABSORPTION	ABSORPTION
Resto	RESTO

Implementado mediante CASE.

**🔹 Identificación de cliente**

Se construyen los siguientes campos garantizando un único valor por llamada:

document_type

document_identification

customer_phone

billing_account_id

Se utilizan:

Agregaciones condicionales

Funciones de ventana

Priorización de valores válidos

**🔹 Flags analíticos**

Según el enunciado ():

masiva_lg

Indica si la llamada pasó por el módulo AVERIA_MASIVA.

info_by_phone_lg

1 si existe step:

CUSTOMERINFOBYPHONE.TX


con step_result = 'OK'

info_by_dni_lg

1 si existe step:

CUSTOMERINFOBYDNI.TX


con step_result = 'OK'

**🔹 Repeated Phone Analysis (24H)**

Campos:

repeated_phone_24H

cause_recall_phone_24H



Se evalúa si el mismo phone_number tiene:

Una llamada en las 24h anteriores

Una llamada en las 24h posteriores

Implementado mediante:

LAG()

LEAD()

Comparaciones de timestamp

Funciones de ventana particionadas por phone_number

## 📈 5️⃣ Tabla ivr_summary

Se crea una tabla resumen con 1 registro por llamada, consolidando todos los indicadores ().

**Campos incluidos:**

ivr_id

phone_number

ivr_result

vdn_aggregation

start_date

end_date

total_duration

customer_segment

ivr_language

steps_module

module_aggregation

document_type

document_identification

customer_phone

billing_account_id

masiva_lg

info_by_phone_lg

info_by_dni_lg

repeated_phone_24H

cause_recall_phone_24H

Esta tabla actúa como tabla de hechos analítica simplificada.

## 🧼 6️⃣ Función clean_integer

Se implementa la función:

clean_integer(input INTEGER)


Regla ():

Si el valor es NULL → devuelve -999999

En caso contrario → devuelve el valor original

Permite estandarizar valores faltantes para explotación analítica.

## 🛠️ Tecnologías Utilizadas

PostgreSQL

BigQuery (para parte analítica)

SQL avanzado:

Window Functions

CASE WHEN

Aggregations

JOIN jerárquicos

Manipulación de fechas

Flags condicionales

## 🎯 Objetivos Técnicos Alcanzados

✔ Modelado relacional correcto
✔ Transformación de datos jerárquicos
✔ Construcción de tabla de detalle
✔ Construcción de tabla resumen
✔ Implementación de lógica de negocio
✔ Uso de funciones de ventana
✔ Creación de funciones SQL

## 🚀 Posibles Mejoras

Implementación de índices para optimización

Separación en esquema staging / mart

Automatización con pipelines ETL

Pruebas unitarias SQL

Validaciones de calidad de datos
