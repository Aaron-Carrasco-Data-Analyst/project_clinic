#  Proyecto ETL con Python y SQL Server

##  Objetivo
Construir un **pipeline de datos** que permita ingerir información desde una base de datos **OLTP (transaccional)** hacia una base de datos de destino optimizada para **Data Warehouse (DW)**, siguiendo el modelo **Kimball (estrella)**.  

Este proyecto busca demostrar cómo implementar un flujo **ETL (Extract, Transform, Load)** usando **Python** y **SQL Server**, con posibilidad de extender hacia herramientas de BI como **Power BI** o **Excel** mediante cubos OLAP.

---

## Estructura de Carpetas


---

## Arquitectura
- **Origen de datos**: Base OLTP (transaccional).
- **Destino**: SQL Server optimizado para DW.
- **Modelo de datos**: Kimball (estrella).
- **Stack tecnológico**:
  - Python (ETL principal).
  - SQL Server (motor de base de datos).


---

## ⚙️ Flujo ETL
1. **Extract**: Conexión a la base OLTP y extracción de tablas relevantes.
2. **Transform**: Limpieza, normalización y modelado de datos según esquema estrella.
3. **Load**: Inserción en SQL Server DW optimizado.
4. **Reporting**: Generación de vistas y conexión OLAP para análisis.

---

## Ejemplo de Modelo Estrella
| Tabla Fáctica | Tablas Dimensión |
|---------------|------------------|
| FactVentas    | DimCliente       |
|               | DimProducto      |
|               | DimTiempo        |
|               | DimSucursal      |

---

## 🚦 Requisitos
- Python 3.9+
- Librerías: `pyodbc`, `pandas`, `sqlalchemy`
- SQL Server (on-premise)
- Opcional: Visual Studio + SSIS, Power BI

---

## 🔧 Instalación
1. Clonar el repositorio:
   ```bash
   git clone https://github.com/usuario/etl_project.git
   cd etl_project