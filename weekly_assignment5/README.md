
# Sales Performance & Trend Analysis Dashboard (Power BI)

## Overview

This project delivers an interactive **Sales Performance & Trend Analysis Dashboard** using **Power BI Desktop and Power BI Service**.
It enables business users to analyze sales performance, track trends, compare dimensions, and make data-driven decisions.

## Objectives

* Analyze **Sales, Profit, and Quantity**
* Track performance trends over time
* Compare **Regions, Categories, and Products**

## Data Model (Star Schema)

* **FactSales**
* **Targets**
* **DimProduct**
* **DimCustomer**
* **DimRegion**

## Transformations

* Removed nulls and duplicates
* Corrected data types
* Split region fields into Country and State
* Created conditional columns for sales bands i.e. High (>100,000), Medium (>50,000), Low (Else)
* Core DAX Measures:

    ```DAX
    Total Sales = SUM(FactSales[SalesAmount])
    Total Profit = SUM(FactSales[Profit])
    Total Quantity = SUM(FactSales[Quantity])
    ```
* Time Intelligence

    ```DAX
    Sales YTD = TOTALYTD([Total Sales], DimDate[Date])
    Sales LY = CALCULATE([Total Sales], SAMEPERIODLASTYEAR(DimDate[Date]))
    YoY Growth % = DIVIDE([Total Sales] - [Sales LY], [Sales LY])
    ```

## Report Pages & Visuals

### Page 1: Executive Dashboard

* KPI Cards: Sales, Profit, YoY Growth
* Donut Chart: Sales by Category
* Ribbon Chart: Category Rank Over Time
* Line Chart: Monthly Sales Trend

### Page 2: Trend Analysis

* Line Chart: Sales & Profit
* Line + Stacked Column: Actual vs Target
* Shape Map: Sales by Region
* Slicers

### Page 3: Product Performance

* Matrix: Category → Subcategory → Product
* Donut Charts
* Conditional formatting
* Top N filters
* Q&A


## Dashboard & Interactivity

* Published to Power BI Service
* Pinned KPIs and visuals to dashboard
* Click actions for page navigation
* Q&A visual enabled
* Advanced Visuals & Dashboard Actions
  


