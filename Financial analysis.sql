
------------------------------------------
-- ADVENTURE WORKS : FINANCIAL ANALYSIS --
------------------------------------------

-- Author: Lisa Schneider
-- Date:
-- Tool used: BigQuery

-----------------------------------------
--------- EXPLORATORY ANALYSIS ----------
-----------------------------------------

-- 1. REVENUE AND SALES DATA
-- 1.1. Overall revenue development over time, segmented by product categories
# While order count has increased massively since 2003-08, revenue has not followed suit in a similar pattern, but has still been increasing.


SELECT FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
    ROUND(SUM(TotalDue)) AS revenue,
    COUNT(DISTINCT SalesOrderID) AS number_of_orders
  FROM `adwentureworks_db.salesorderheader`
GROUP BY FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH))
ORDER BY order_month;

# After going to an Open Session, I found that TotalDue is not the best column to calculate revenue. So I have to work with OrderQty * UnitPrice instead. 


# Sales jump in 2003-08 mostly driven by bikes and component category. Also some indication of seasonal effects with similar sales spike in 2002-08.

SELECT
Name,
order_month,
SUM(OrderQty * UnitPrice) AS revenue
FROM(
  SELECT category.Name
  , FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(salesheader.OrderDate, MONTH)) AS order_month
  , salesdetail.OrderQty
  , salesdetail.UnitPrice
  FROM `adwentureworks_db.salesorderheader` AS salesheader
  LEFT JOIN `adwentureworks_db.salesorderdetail` AS salesdetail USING (SalesOrderID)
  LEFT JOIN `adwentureworks_db.product` AS product USING (ProductID)
  LEFT JOIN `adwentureworks_db.productsubcategory` AS subcategory USING (ProductSubcategoryID)
  LEFT JOIN `adwentureworks_db.productcategory` AS category USING (ProductCategoryID)
  ORDER BY category.Name, order_month)
GROUP BY name, order_month
ORDER BY name, order_month;


-- 1.2. Gross Profit Margin per product category
# Result is not feasible - margins are in the 90% so must be an error. 
WITH cogs_table AS (SELECT 
  order_month,
  SalesOrderID,
  Name,
  TotalDue,
  ROUND(StandardCost * OrderQty) AS cogs
FROM (
  SELECT header.SalesOrderID,
  FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(header.OrderDate, MONTH)) AS order_month,
  product.ProductID,
  header.TotalDue,
  detail.OrderQty,
  product.StandardCost,
  cat.Name
  FROM `adwentureworks_db.salesorderheader` AS header
  LEFT JOIN `adwentureworks_db.salesorderdetail` AS detail ON header.SalesOrderID = detail.SalesOrderID
  LEFT JOIN `adwentureworks_db.product` AS product ON detail.ProductID = product.ProductID
  LEFT JOIN `adwentureworks_db.productsubcategory` AS sub ON product.ProductSubcategoryID = sub.ProductSubcategoryID
  LEFT JOIN `adwentureworks_db.productcategory` AS cat ON sub.ProductCategoryID = cat.ProductCategoryID)
)

SELECT order_month,
Name,
SUM(TotalDue - cogs) / SUM(TotalDue) AS gross_profit_margin
FROM cogs_table
GROUP BY order_month, Name
ORDER BY Name, order_month;

-- Trying another way to join these tables together to understand gross profit margin.
# Result gave more reasonable values of the margin. Since one sales order includes multiple products, several category names apply to one order and causes duplicate rows. The calculation with TotalDue is therefore not providing reliable results for categories - need to claclulate revenue from OrderQty * UnitPrice.  

WITH cogs_table AS (
SELECT SalesOrderID,
SUM(OrderQty * StandardCost) AS cogs
FROM (
  SELECT 
  detail.SalesOrderID,
  detail.OrderQty,
  product.StandardCost
  FROM `adwentureworks_db.salesorderdetail` AS detail
  LEFT JOIN `adwentureworks_db.product` AS product ON detail.ProductID = product.ProductID
  LEFT JOIN `adwentureworks_db.productsubcategory` AS sub ON product.ProductSubcategoryID = sub.ProductSubcategoryID
  LEFT JOIN `adwentureworks_db.productcategory` AS cat ON sub.ProductCategoryID = cat.ProductCategoryID)
GROUP BY SalesOrderID
ORDER BY SalesOrderID),

revenue_table AS (
  SELECT SalesOrderID,
    FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
    SUM(TotalDue) AS Revenue
  FROM `adwentureworks_db.salesorderheader`
  GROUP BY SalesOrderID, order_month
)

SELECT 
  rev.SalesOrderID,
  rev.order_month,
  SUM(rev.Revenue - cog.cogs) / SUM(rev.Revenue) AS gross_profit_margin
FROM revenue_table AS rev
LEFT JOIN cogs_table AS cog ON rev.SalesOrderID = cog.SalesOrderID
GROUP BY   rev.SalesOrderID, rev.order_month


-- 1.2. Year-over-year, quarter-over-quarter and month-over-month sales growth
# Sales growth flunctuating a lot from one month to another. 

WITH sales_by_month AS (
    SELECT  
        FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
        ROUND(SUM(TotalDue),0) AS total_sales  
    FROM `adwentureworks_db.salesorderheader`  
    GROUP BY order_month
),
sales_growth AS (
    SELECT order_month,
        total_sales,
        LAG(total_sales) OVER (ORDER BY order_month) AS previous_month_sales  
    FROM sales_by_month  
)
SELECT order_month,
    total_sales,
    previous_month_sales,
    (total_sales - previous_month_sales) / previous_month_sales * 100 AS sales_growth_percentage  
FROM sales_growth  
WHERE previous_month_sales IS NOT NULL
ORDER BY order_month;

# Sales growth quarter-over-quarter also shows seasonality trends with Q4 and Q1 generally dropping to negative sales growth. 
WITH sales_by_quarter AS (
    SELECT  
        FORMAT_DATETIME('%Y-Q%Q', DATETIME_TRUNC(OrderDate, QUARTER)) AS order_quarter,
        ROUND(SUM(TotalDue), 0) AS total_sales  
    FROM `adwentureworks_db.salesorderheader`  
    GROUP BY order_quarter  
    ORDER BY order_quarter  
),
sales_growth AS (
    SELECT 
        order_quarter,
        total_sales,
        LAG(total_sales) OVER (ORDER BY order_quarter) AS previous_quarter_sales  
    FROM sales_by_quarter  
)
SELECT 
    order_quarter,
    total_sales,
    previous_quarter_sales,
    (total_sales - previous_quarter_sales) / previous_quarter_sales * 100 AS sales_growth_percentage  
FROM sales_growth  
WHERE previous_quarter_sales IS NOT NULL  
ORDER BY order_quarter;

# Comparing sales growth by month to same month previous year shows that growth is steadily positive except for the last month where it dips below 0 (July 2004).
WITH sales_by_month AS (
    SELECT  
        FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
        ROUND(SUM(TotalDue), 0) AS total_sales  
    FROM `adwentureworks_db.salesorderheader`  
    GROUP BY order_month
),
sales_growth AS (
    SELECT 
        order_month,
        total_sales,
        LAG(total_sales) OVER (PARTITION BY SUBSTR(order_month, 6, 2) # Extracting the month part and LAG to extract previous month
            ORDER BY order_month
        ) AS previous_year_sales  
    FROM sales_by_month  
)
SELECT 
    order_month,
    total_sales,
    previous_year_sales,
    (total_sales - previous_year_sales) / previous_year_sales * 100 AS sales_growth_percentage  
FROM sales_growth  
WHERE previous_year_sales IS NOT NULL  
ORDER BY order_month;

-- 1.3. Sales development by channel
# Online selling increased since 08-2003

SELECT FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
  CASE WHEN SalesPersonID IS NULL THEN 'ONLINE' ELSE 'STORE' END AS channel,
  ROUND(SUM(TotalDue)) AS total_amount
FROM `adwentureworks_db.salesorderheader` 
GROUP BY ALL
ORDER BY channel, order_month;

-- Trying to understand if individual customers and vendors can be differented in the sales table. Starting with below code for individual customers. 
SELECT 
  header.CustomerID,
  individual.CustomerID,
  SUM(header.TotalDue) AS sales
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.individual` AS individual USING (CustomerID)
GROUP BY   header.CustomerID, individual.CustomerId;
# 19 119 rows of customerIDs in sales table

SELECT 
  header.CustomerID,
  individual.CustomerID AS ind_CustomerID,
  SUM(header.TotalDue) AS sales
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.individual` AS individual USING (CustomerID)
WHERE individual.CustomerID IS NOT NULL
GROUP BY   header.CustomerID, individual.CustomerId;
# 18 484 rows when removing those who do not have a customerID in individuals table (likely vendors). Individual customers could therefory maybe identified by a filter like 'orders from CustomerID that exist in individuals table'

-- Trying the same as above but to identify vendors.
SELECT 
  header.ContactID,
  vendor.ContactID AS vendor_contact,
  SUM(header.TotalDue) AS sales
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.vendorcontact` AS vendor USING (ContactID)
GROUP BY header.ContactID, vendor.ContactID;
# 19119 rows

SELECT 
  header.ContactID,
  vendor.ContactID AS vendor_contact,
  SUM(header.TotalDue) AS sales
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.vendorcontact` AS vendor USING (ContactID)
WHERE vendor.ContactID IS NOT NULL
GROUP BY header.ContactID, vendor.ContactID;
# 0 rows when trying to extract the contacts from sales table that also exist in the vendor table. It does not seem so easy to identify vendor purchases. 

SELECT 
  header.CustomerID,
  individual.CustomerID AS ind_CustomerID,
  contact.ContactID,
  vendor.VendorID,
  SUM(header.TotalDue) AS sales
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.individual` AS individual ON header.CustomerID = individual.CustomerID
LEFT JOIN `adwentureworks_db.contact` AS contact ON header.ContactID = contact.ContactID
LEFT JOIN `adwentureworks_db.vendorcontact` AS vendor ON contact.ContactID = vendor.ContactID
WHERE individual.CustomerID IS NULL
  AND vendor.VendorID IS NOT NULL
GROUP BY   header.CustomerID, individual.CustomerId, contact.ContactID, vendor.VendorID;
# When trying to see whether the orders without CustomerID in the individuals table have a contactID that exists in the vendorcontact table, also here we get 0 rows. So it is not straightforward to identify vendors and their purchases from the sales table. 

SELECT 
  header.AccountNumber,
  vendor.AccountNumber AS vendor_acc_number,
  SUM(header.TotalDue) AS sales
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.vendor` AS vendor USING (AccountNumber)
WHERE vendor.AccountNumber IS NOT NULL
GROUP BY header.AccountNumber, vendor.AccountNumber;
# Also here 0 rows when trying to compare AccountNumber data between sales and vendor table. 


SELECT 
  header.CustomerID,
  store.CustomerID AS store_custID,
  ROUND(SUM(header.TotalDue),0) AS sales
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.storecontact` AS store USING (CustomerID)
GROUP BY header.CustomerID,
  store.CustomerID;
# 19119 rows trying to understand if CustomerID can be connected to CustomerID in storecontact table

SELECT 
  header.CustomerID,
  store.CustomerID AS store_custID,
  ROUND(SUM(header.TotalDue),0) AS sales
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.storecontact` AS store USING (CustomerID)
WHERE store.CustomerID IS NOT NULL
GROUP BY header.CustomerID,
  store.CustomerID;
# 635 records where CustomerID in sales table matches CustomerID in store contact, could be potentially identified as purchases from resellers. 
# 18 484 individual customers + 635 resellers = 19119 (total of all individual customers); so customer type could be identified this way.  


-- 1.4. Sales development by region/ country
# US strongest market consistently; CA was stronger until 08-2003 while other markets have picked up and further increased since then. Maybe related to changes in channel sales?
SELECT FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(salesheader.OrderDate, MONTH)) AS order_month,
  ROUND(SUM(salesheader.TotalDue)) AS revenue,
  territory.CountryRegionCode
FROM `adwentureworks_db.salesorderheader` AS salesheader
LEFT JOIN `adwentureworks_db.salesterritory` AS territory USING (TerritoryID)
GROUP BY territory.CountryRegionCode, order_month
ORDER BY territory.CountryRegionCode, order_month;


-- 2. COST AND EXPENSE DATA
-- 2.1 Cost of Goods Sold (COGS)
# COGS following similar pattern as sales trend


WITH historic_cost_table AS (
SELECT
  FORMAT_DATETIME('%Y-%m', DATETIME_TRUNC(OrderDate, MONTH)) AS order_month,
  ProductID,
  SUM(OrderQty) AS total_qty,
  StandardCost
FROM (
  SELECT salesheader.OrderDate,
  salesdetail.ProductID,
  salesheader.SalesOrderID,
  salesdetail.OrderQty,
  history.StandardCost,
  history.StartDate,
  history.EndDate
  FROM `adwentureworks_db.salesorderheader` AS salesheader
  LEFT JOIN `adwentureworks_db.salesorderdetail` AS salesdetail USING (SalesOrderID)
  LEFT JOIN `adwentureworks_db.product` AS product USING (ProductID)
  LEFT JOIN `adwentureworks_db.productcosthistory` AS history ON product.ProductID = history.ProductID
    AND salesheader.OrderDate >= CAST(history.StartDate AS TIMESTAMP) 
    AND (salesheader.OrderDate <= CAST(history.EndDate AS TIMESTAMP) OR history.EndDate IS NULL) # Joining table retrieving only the applicable StandardCost based on when in time the order was placed
  ORDER BY salesdetail.ProductID)
GROUP BY order_month, ProductID, StandardCost
ORDER BY ProductID),

COGS_table AS (
SELECT 
  ProductID,
  order_month,
  total_qty,
  StandardCost,
  total_qty * StandardCost AS COGS
FROM historic_cost_table
ORDER BY ProductID, order_month)

SELECT order_month,
SUM(total_qty) AS qty,
ROUND(SUM(COGS),0) AS total_cogs
FROM COGS_table
GROUP BY order_month
ORDER BY order_month;


-- 3. ACCOUNTS RECEIVABLE AND PAYABLE
# Tried to calculate accounts receivable as a first measure in this section but the result is not reliable  and likely incorrect (0.25)
WITH total_sales AS (
    SELECT 
        SUM(TotalDue) AS total_sales,
        COUNT(DISTINCT SalesOrderID) AS total_orders
    FROM `adwentureworks_db.salesorderheader`
  WHERE FORMAT_DATETIME('%Y', DATETIME_TRUNC(OrderDate, MONTH)) = '2004'
  AND CreditCardID IS NOT NULL
),
receivables AS (
    SELECT 
        SUM(TotalDue) AS total_receivables
    FROM `adwentureworks_db.salesorderheader`
    WHERE CAST(DueDate AS TIMESTAMP) > '2004-07-31'
    AND FORMAT_DATETIME('%Y', DATETIME_TRUNC(OrderDate, MONTH)) = '2004'
    AND CreditCardID IS NOT NULL
)
SELECT 
    (r.total_receivables / ts.total_sales) * 365 AS days_sales_outstanding
FROM 
    total_sales ts,
    receivables r;


-- 4. Inventory Management
-- Inventory turnover is difficult to calculate as we do not have continuous stock levels in the data.

# 95 products have lower stock level than SafetyStockLevel which could be shown as a percentage KPI.
SELECT product.ProductID
FROM `adwentureworks_db.product` AS product
LEFT JOIN `adwentureworks_db.productinventory` AS inventory USING (ProductID)
WHERE product.SafetyStockLevel < inventory.Quantity


-- DATA PREPARATION AND EXTRACTION

-- Adding the customer type to the salesorderheader table
SELECT *,
COUNT(*) AS dup_count
FROM (
SELECT 
header.SalesOrderID,
header.CustomerID,
ind.CustomerID,
store.CustomerID,
CASE 
  WHEN ind.CustomerID IS NOT NULL THEN 'individual'
  WHEN store.CustomerID IS NOT NULL THEN 'store'
  ELSE 'unknown'
END AS customer_type
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN `adwentureworks_db.individual` AS ind ON header.CustomerID = ind.CustomerID
LEFT JOIN `adwentureworks_db.storecontact` AS store ON header.CustomerID = store.CustomerID)
GROUP BY ALL
ORDER BY dup_count DESC
# 31 795 rows instead of 31 465 (unique sales orders) so duplicates that need to be investigated. The duplicated rows seem to be mainly store customer types.
# In the storecontact table, I found that one CustomerID can have multiple ContactIDs which likely causes the issue. 
# Solved the issue by using CTEs that aggregate by CustomerID from the individual and storecontact table before joining the data. Result is now 31 465 rows, 

WITH unique_store AS (
  SELECT CustomerID
  FROM `adwentureworks_db.storecontact`
  GROUP BY CustomerID
),
unique_individual AS (
  SELECT CustomerID
  FROM `adwentureworks_db.individual`
  GROUP BY CustomerID
)

SELECT 
  header.*,
  CASE 
    WHEN ind.CustomerID IS NOT NULL THEN 'individual'
    WHEN store.CustomerID IS NOT NULL THEN 'store'
    ELSE 'unknown'
  END AS customer_type
FROM `adwentureworks_db.salesorderheader` AS header
LEFT JOIN unique_individual AS ind ON header.CustomerID = ind.CustomerID
LEFT JOIN unique_store AS store ON header.CustomerID = store.CustomerID;

-- Adjusting salesorderdetail table with StandardCost at time of purchase. 

SELECT
    salesdetail.*,
    history.StandardCost AS StandardCostAtPurchase
  FROM `adwentureworks_db.salesorderheader` AS salesheader
  LEFT JOIN `adwentureworks_db.salesorderdetail` AS salesdetail USING (SalesOrderID)
  LEFT JOIN `adwentureworks_db.productcosthistory` AS history ON salesdetail.ProductID = history.ProductID
    AND salesheader.OrderDate >= CAST(history.StartDate AS TIMESTAMP) 
    AND (salesheader.OrderDate <= CAST(history.EndDate AS TIMESTAMP) OR history.EndDate IS NULL)

-- Adding category and subcategory details to product table (504 rows)

SELECT 
  product.ProductID,
  product.Name AS product_name,
  product.ProductNumber,
  product.SafetyStockLevel,
  product.StandardCost,
  product.ListPrice,
  product.Size,
  product.SellStartDate,
  product.SellEndDate,
  product.DiscontinuedDate,
  cat.ProductCategoryID,
  cat.Name AS category_name,
  sub.ProductSubcategoryID,
  sub.Name AS subcategory_name
FROM `adwentureworks_db.product` AS product
LEFT JOIN `adwentureworks_db.productsubcategory` AS sub ON product.ProductSubcategoryID = sub.ProductSubcategoryID
LEFT JOIN `adwentureworks_db.productcategory` AS cat ON sub.ProductCategoryID = cat.ProductCategoryID;


-- Preparing SpecialOffer table to calculate discount rates

SELECT
prod.ProductID,
offer.*
FROM `adwentureworks_db.specialofferproduct` AS prod
LEFT JOIN `adwentureworks_db.specialoffer` AS offer USING (SpecialOfferID)

-- Extracting ProductInventory table to calculate GMROI (Gross Margin Return On Inventory)

SELECT *
FROM `adwentureworks_db.productinventory`
