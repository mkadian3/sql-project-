
select * from walmartsales_dataset;

SELECT Branch, (SUM(Total) - LAG(SUM(Total)) 
OVER (PARTITION BY Branch ORDER BY Date)) /NULLIF(LAG(SUM(Total)) 
OVER (PARTITION BY Branch ORDER BY Date), 0) AS growth_rate 
FROM walmartsales_dataset GROUP BY Branch, Date 
ORDER BY growth_rate DESC LIMIT 1;

SELECT Branch, Product_line, SUM(gross_income - cogs) AS total_profit 
FROM walmartsales_dataset
GROUP BY Branch, Product_line 
ORDER BY Branch, total_profit DESC;


WITH CustomerSpending AS (
    SELECT Customer_ID, SUM(Total) AS TotalSpending
    FROM walmartsales_dataset GROUP BY Customer_ID
),
SpendingRank AS (
    SELECT Customer_ID, TotalSpending, NTILE(3) OVER (ORDER BY TotalSpending DESC) AS SpendingTier
    FROM CustomerSpending
)
SELECT Customer_ID, CASE WHEN SpendingTier = 1 THEN 'High'WHEN SpendingTier = 2 THEN 'Medium'
ELSE 'Low'
END AS SpendingCategory
FROM SpendingRank;

WITH ProductStats AS (
    SELECT Product_line, AVG(Total) AS AvgSales, STDDEV(Total) AS StdDevSales
    FROM walmartsales_dataset GROUP BY Product_line
)
SELECT ws.Invoice_ID, ws.Product_line, ws.Total
FROM walmartsales_dataset ws
JOIN ProductStats ps
ON ws.Product_line = ps.Product_line
WHERE ws.Total < (ps.AvgSales - 3 * ps.StdDevSales)
OR ws.Total > (ps.AvgSales + 3 * ps.StdDevSales);

WITH PaymentStats AS (SELECT City, Payment, COUNT(*) AS PaymentCount
FROM walmartsales_dataset GROUP BY City, Payment
),
RankedPayments AS (
SELECT City, Payment, PaymentCount, RANK() OVER (PARTITION BY City ORDER BY PaymentCount DESC) AS RankByPayment
FROM PaymentStats
)
SELECT City, Payment FROM RankedPayments WHERE RankByPayment = 1;


SELECT MONTH(Date) AS Month, Gender, SUM(Total) AS TotalSales
FROM walmartsales_dataset
GROUP BY MONTH(Date), Gender
ORDER BY Month, Gender;


SELECT Customer_type, Product_line, SUM(Total) AS TotalSales
     FROM walmartsales_dataset
          GROUP BY Customer_type, Product_line ORDER BY Customer_type, TotalSales DESC;


WITH PurchaseHistory AS (
SELECT Customer_ID, Date AS PurchaseDate, 
LEAD(Date) OVER (PARTITION BY Customer_ID ORDER BY Date) AS NextPurchaseDate FROM walmartsales_dataset
)
SELECT Customer_ID, COUNT(*) AS RepeatCount
FROM PurchaseHistory WHERE DATEDIFF(NextPurchaseDate, PurchaseDate) <= 30
GROUP BY Customer_ID HAVING RepeatCount > 1;

SELECT Customer_ID, SUM(Total) AS TotalSales
FROM walmartsales_dataset
GROUP BY Customer_ID
ORDER BY TotalSales DESC
LIMIT 5;


SELECT DAYNAME(Date) AS DayOfWeek, 
       SUM(Total) AS TotalSales
FROM walmartsales_dataset
GROUP BY DAYNAME(Date)
ORDER BY FIELD(DAYNAME(Date), 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');



WITH Sales_By_Day AS (
    SELECT
        DAYNAME(STR_TO_DATE(Date, '%Y-%m-%d')) AS Day_Of_Week,  
        SUM(Total) AS TotalSales
    FROM
        Walmartsales_Dataset
    GROUP BY
        DAYNAME(STR_TO_DATE(Date, '%Y-%m-%d'))
)
SELECT
    Day_Of_Week,
    TotalSales
FROM
    Sales_By_Day
WHERE
    Day_Of_Week IS NOT NULL
ORDER BY
    TotalSales;





































