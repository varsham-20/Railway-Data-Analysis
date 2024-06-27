create database Railway

use Railway

Create Table Railway_Data_Analysis
(Transaction_ID varchar(max),
Date_of_Purchase varchar(max),
Time_of_Purchase varchar(max),
Purchase_Type varchar(max),
Payment_Method varchar(max),
Railcard varchar(max),
Ticket_Class varchar(max),
Ticket_Type varchar(max),
Price varchar(max),
Departure_Station varchar(max),
Arrival_Destination varchar(max),
Date_of_Journey varchar(max),
Departure_Time varchar(max),
Arrival_Time varchar(max),
Actual_Arrival_Time varchar(max),
Journey_Status varchar(max),
Reason_for_Delay varchar(max),
Refund_Request varchar(max));

select * from Railway_Data_Analysis

drop table Railway_Data_Analysis

select column_name, data_type
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME = 'Railway_data_analysis'

bulk insert Railway_Data_Analysis
from 'D:\Data_Analysis_proj\railway.csv'
with (fieldterminator=',',rowterminator='\n', firstrow=2, maxerrors=40)

--To remove duplicates
with rmv_duplicate as(select *, row_number() over(partition by Transaction_ID order by Transaction_ID) as Transactions
from Railway_Data_Analysis)
select * from rmv_duplicate
where Transactions>1

--There are no duplictae

-- Step 1: Identify invalid Price values
SELECT Transaction_ID, Price
FROM Railway_Data_Analysis
WHERE ISNUMERIC(Price) = 0;

update Railway_Data_Analysis set Price = '4'
where price = '4ú' --updtaed 4 invalid datas in price

select date_of_purchase from railway_data_analysis
WHERE date_of_purchase LIKE '31-12%2023';


UPDATE railway_data_analysis
SET date_of_purchase = REPLACE(date_of_purchase, '%2023', '-2023')
WHERE date_of_purchase LIKE '31-12%2023';


SELECT *
FROM Railway_Data_Analysis
WHERE reason_for_delay IS NULL OR reason_for_delay = '';

UPDATE Railway_Data_Analysis
SET Actual_Arrival_Time = '00:00:00'
WHERE Actual_Arrival_Time IS NULL;

UPDATE Railway_Data_Analysis
SET reason_for_delay = 'No Delay'
WHERE reason_for_delay = 'NULL';

select distinct reason_for_delay
from railway_data_analysis

-----------------------------------------------------------------------------------------------------------------------------

/*1.	Identify Peak Purchase Times and Their Impact on Delays: This query determines the peak times for ticket purchases and analyzes 
if there is any correlation with journey delays.*/

WITH ValidData AS (
    SELECT 
        Transaction_ID,
        Date_of_Purchase,
        Time_of_Purchase,
        Arrival_Time,
        Actual_Arrival_Time,
        DATEPART(HOUR, TRY_CAST(Time_of_Purchase AS TIME)) AS Hour_Of_Day,
        TRY_CAST(Arrival_Time AS TIME) AS Arrival_Time_Cast,
        TRY_CAST(Actual_Arrival_Time AS TIME) AS Actual_Arrival_Time_Cast
    FROM 
        Railway_Data_Analysis
),
Purchase_Times AS (
    SELECT 
        Hour_Of_Day,
        COUNT(*) AS Purchase_Count
    FROM 
        ValidData
    WHERE 
        Hour_Of_Day IS NOT NULL
    GROUP BY 
        Hour_Of_Day
),
Delays AS (
    SELECT 
        Hour_Of_Day,
        COUNT(CASE 
            WHEN Actual_Arrival_Time_Cast > Arrival_Time_Cast THEN 1 
            ELSE NULL 
        END) AS Delayed_Journeys,
        COUNT(*) AS Total_Journeys
    FROM 
        ValidData
    WHERE 
        Hour_Of_Day IS NOT NULL
        AND Arrival_Time_Cast IS NOT NULL
        AND Actual_Arrival_Time_Cast IS NOT NULL
    GROUP BY 
        Hour_Of_Day
)
SELECT 
    PT.Hour_Of_Day,
    PT.Purchase_Count, 
    D.Delayed_Journeys,
    D.Total_Journeys,
    (D.Delayed_Journeys * 100.0 / NULLIF(D.Total_Journeys, 0)) AS Delay_Percentage
FROM 
    Purchase_Times PT
JOIN 
    Delays D ON PT.Hour_Of_Day = D.Hour_Of_Day
ORDER BY 
    PT.Purchase_Count desc;

/* Hour_Of_Day - represents the hour of the day (0-23)(24 hours format) 
Peak Purchase Hours:
17:00: The highest number of purchases (2,740), with a delay percentage of approximately 6.82%.
20:00: The second-highest number of purchases (2,239), with a delay percentage of approximately 1.47%.
09:00: The third-highest number of purchases (2,070), with a significant delay percentage of approximately 22.22%.
07:00: The fourth-highest number of purchases (2,046), with a delay percentage of approximately 5.72%.
08:00: Another peak time with 2,008 purchases and a low delay percentage of approximately 1.20%.

Hours with High Delay Percentages:
09:00: The highest delay percentage (22.22%) among the hours with a high purchase count.
06:00: A delay percentage of approximately 18.48% with 1,910 purchases.
16:00: Another significant delay percentage (15.15%) with 1,056 purchases.
10:00: Notable delay percentage (13.31%) with 1,187 purchases.
12:00: Delay percentage of approximately 10.24% with 1,025 purchases.
02:00: Delay percentage of approximately 10.12% with 642 purchases.

Correlation Between Purchases and Delays:
There is no consistent pattern where higher purchase counts directly correlate with higher delays. For instance,
while 09:00 has a high delay percentage with a high purchase count, 17:00 also has a high purchase count but a much lower delay percentage (6.82%).
Some hours with relatively fewer purchases, like 06:00 and 16:00, exhibit high delay percentages,
indicating that other factors beyond purchase volume might influence delays.

Low Delay Hours:
Hours like 23:00 and 21:00 have no delays despite a moderate number of purchases, suggesting these times might be more efficient
in handling journeys without delays.
The early morning hours (00:00 and 03:00) and late night hours (20:00) also show low delay percentages,
making these times potentially better for on-time performance.

The analysis indicates that peak purchase hours do not always correlate with higher journey delays. Specific times like 09:00 and 06:00 
show significant delays despite high purchase volumes, while other peak times like 17:00 and 20:00 manage to maintain relatively low delay percentages.
This suggests that while purchase volume might have some impact, other operational factors are likely influencing the delay rates significantly.
*/
--------------------------------------------------------------------------------------------------------------------------------------

/* 2. Analyze Journey Patterns of Frequent Travelers: This query identifies frequent travelers (those who made more than three purchases) and 
analyzes their most common journey patterns */

select * from Railway_Data_Analysis

WITH Frequent_Travelers AS (
    SELECT
        Payment_Method,
        Railcard,
        Departure_Station,
        COUNT(*) AS Purchase_Count
    FROM 
        Railway_Data_Analysis
    GROUP BY 
        Payment_Method,
        Railcard,
        Departure_Station
    HAVING 
        COUNT(*) > 3
)

SELECT
    r.Payment_Method,
    r.Railcard,
    r.Departure_Station,
    r.Ticket_Class,
    r.Ticket_Type,
    COUNT(*) AS Ticket_Count
FROM 
    Railway_Data_Analysis r
INNER JOIN 
    Frequent_Travelers f
ON 
    r.Payment_Method = f.Payment_Method
    AND r.Railcard = f.Railcard
    AND r.Departure_Station = f.Departure_Station
GROUP BY 
    r.Payment_Method,
    r.Railcard,
    r.Departure_Station,
    r.Ticket_Class,
    r.Ticket_Type
HAVING 
    COUNT(*) > 3
ORDER BY 
    Ticket_Count DESC;


/* There is no customer id for this query to predict exact journey patterns of a frequent travelers. But still predicted based on the railcards,
payment_method, departure station. Some times arrival destination might differ. 

Analysis based on this 3 factors:
Payment Method:
The majority of the frequent travelers used Credit Card and Contactless payment methods.
Credit Card is used in 88% of the transactions listed, showing a clear preference.
Railcard:
The majority of travelers do not use a railcard, indicating that railcards are less common among frequent travelers.
Departure Station:
London Paddington is the most common departure station, followed by Liverpool Lime Street, Manchester Piccadilly, London Kings Cross,
and London St Pancras.
London stations collectively dominate the departure points for frequent travelers, reflecting high travel activity in and out of the capital.

Journey Patterns:
The combination of Credit Card payment, no railcard, and departure from major London stations in Standard class with Advance tickets is 
the most frequent journey pattern.
Manchester Piccadilly and Liverpool Lime Street are also key hubs for frequent travelers.
Frequent departures from London Paddington, Liverpool Lime Street, London Kings Cross, London Euston and Manchester Piccadilly.
Most common tickets type: Standard Advance, Off-Peak, and Anytime.
Standard ticket class is overwhelmingly preferred (around 96% of entries).
First Class tickets are less common but still notable among certain frequent travelers, particularly those using Credit Cards.
*/

------------------------------------------------------------------------------------------------------------------------------------------------

/* 3. Revenue Loss Due to Delays with Refund Requests: This query calculates the total revenue loss due to delayed journeys for 
which a refund request was made. */

ALTER TABLE Railway_Data_Analysis
ALTER COLUMN Price DECIMAL(10, 2);

select count(Refund_Request) from Railway_Data_Analysis
where refund_request = 'YES' and journey_status = 'delayed'

select * from railway_data_analysis

-- Calculate Total Revenue Loss Due to Delays with Refund Requests

WITH Refunds AS (
    SELECT 
        Date_of_Journey,
        Departure_Station,
        Arrival_Destination,
        Journey_Status,
        Refund_Request,
        TRY_CAST(Price AS DECIMAL(10, 2)) AS Amount_Refunded
    FROM 
        Railway_Data_Analysis
    WHERE 
        Journey_Status = 'Delayed'
        AND Refund_Request = 'Yes'
),
TotalRevenue AS (
    SELECT 
        SUM(TRY_CAST(Price AS DECIMAL(10, 2))) AS Total_Revenue
    FROM 
        Railway_Data_Analysis
)
SELECT 
    R.Date_of_Journey,
    R.Departure_Station,
    R.Arrival_Destination,
    R.Journey_Status,
    R.Refund_Request,
    R.Amount_Refunded,
    TR.Total_Revenue,
    (SELECT SUM(Amount_Refunded) FROM Refunds) AS Total_Revenue_Loss
FROM 
    Refunds R,
    TotalRevenue TR;


/* Total Revenue: The total revenue generated from ticket sales, which is 741,921.00.
Revenue Loss: indicates the amount of revenue that was refunded due to delays in journeys. 
When passengers experience delays significant enough to request refunds, this amount is 26,165.00 units.

Total revenue loss due to delayed trains in 26165.00. That is 3-4% percentange of the total revenue.
By analyzing the revenue loss due to delays with refund requests, the railways can gain valuable insights into the 
financial impact of service delays and take informed actions to mitigate these losses. Improving the punctuality of services and
reviewing refund policies can help reduce the revenue loss and enhance overall customer satisfaction.
*/

-------------------------------------------------------------------------------------------------------------------------------------

/* 4. Impact of Railcards on Ticket Prices and Journey Delays: This query analyzes the average ticket price and 
delay rate for journeys purchased with and without railcards. */

SELECT
    Railcard_Type,
    AVG(CAST(Price AS DECIMAL(10, 2))) AS Average_Ticket_Price,
    SUM(CASE WHEN Journey_Status = 'Delayed' THEN 1 ELSE 0 END) AS Delayed_Journey_Count,
    COUNT(*) AS Total_Journey_Count,
    CAST(SUM(CASE WHEN Journey_Status = 'Delayed' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 AS Delay_Rate_Percentage
FROM (
    SELECT
        CASE 
            WHEN Railcard = 'Disabled' THEN 'Disabled Railcard'
            WHEN Railcard = 'Adult' THEN 'Adult Railcard'
            WHEN Railcard = 'Senior' THEN 'Senior Railcard'
            ELSE 'No Railcard'
        END AS Railcard_Type,
        Price,
        Journey_Status
    FROM
        Railway_Data_Analysis
) AS Subquery
GROUP BY
    Railcard_Type
ORDER BY
    Delay_Rate_Percentage DESC;

/* 
Impact on Average Ticket Price:
Adult Railcard: The average ticket price is 17.81 units. Compared to other railcards, journeys using the Adult Railcard have a
relatively higher average ticket price.
No Railcard: Shows the highest average ticket price among all the railcard types, with average costing of 27.43 units.
Disabled Railcard: The average ticket price is 16.92 units, which is lower than the Adult Railcard and No Railcard categories.
Senior Railcard: Shows the lowest average ticket price among all the types, with journeys costing approximately 10.58 units on average.

Impact on Journey Delays:
Adult Railcard: Has a delay rate percentage of 13.72%. This means that 13.72% of journeys purchased with an Adult Railcard experienced delays.
No Railcard: Shows a delay rate percentage of 6.74%, indicating a lower proportion of delayed journeys compared to journeys with railcards.
Disabled Railcard: Has a delay rate of 4.40%, suggesting a relatively lower incidence of delays for journeys using this railcard type.
Senior Railcard: Shows the lowest delay rate among the categories analyzed, with only 2.89% of journeys experiencing delays.

Insights:
Ticket Price: Railcards influence the average ticket price significantly. Generally, journeys with Senior Railcards have the lowest ticket prices,
while those without any railcard have the highest.

Impact on Delays: There is a noticeable variation in delay rates across different railcard types. Journeys with Adult Railcards, 
despite having 2nd higher prices, also experience a higher rate of delays compared to other categories.

Considerations: Passengers may consider not only the cost savings from using railcards but also the associated delay risks when choosing
their ticket type.
*/
---------------------------------------------------------------------------------------------------------------------

/* 5. Journey Performance by Departure and Arrival Stations: This query evaluates the performance of journeys by 
calculating the average delay time for each pair of departure and arrival stations. */

SELECT 
    Departure_Station,
    Arrival_Destination,
	Reason_for_Delay,
    AVG(DATEDIFF(MINUTE, CAST(Arrival_Time AS TIME), CAST(Actual_Arrival_Time AS TIME))) AS Average_Delay_Time_In_Minutes
FROM 
    Railway_Data_Analysis
WHERE 
    Journey_Status = 'Delayed'
GROUP BY 
    Departure_Station, 
    Arrival_Destination,
	Reason_for_Delay
ORDER BY
	Average_Delay_Time_In_Minutes desc;

select distinct departure_station, arrival_destination
from railway_data_analysis

/* Stations with Highest Average Delays:
Manchester Piccadilly to Leeds: Signal Failure (143 minutes) - This route experiences the highest average delay time due to signal failures.
Manchester Piccadilly to Liverpool Lime Street: Staff Shortage (100 minutes) - Significant delays are attributed to staffing issues on this route.
York to Doncaster: Traffic (89 minutes) - This route faces substantial delays primarily due to traffic conditions.
London Euston to Birmingham New Street: Weather (82 minutes) - Weather-related issues contribute significantly to delays on this route.

Impact on Journey Performance:
These insights help railway operators understand the critical areas where delays are most frequent and severe, enabling them to prioritize
improvements and allocate resources more effectively.
Understanding the reasons behind delays (e.g., infrastructure issues, staffing, weather) allows for targeted interventions 
to minimize disruptions and improve overall service reliability.

It provides valuable insights into the performance of train journeys across different station pairs, highlighting the reasons for delays
and the average delay times associated with each route. These insights are crucial for enhancing operational efficiency, improving service 
reliability, and ultimately enhancing the passenger experience in the railway sector.
*/

----------------------------------------------------------------------------------------------------------------------------

/* 6. Revenue and Delay Analysis by Railcard and Station
This query combines revenue analysis with delay statistics, providing insights into journeys' performance and 
revenue impact involving different railcards and stations. */

	SELECT
    rda.Railcard AS Railcard_Type,
    rda.Departure_Station,
    rda.Arrival_Destination,
    COUNT(*) AS Journey_Count,
    AVG(CASE WHEN ISNUMERIC(rda.Price) = 1 THEN CAST(rda.Price AS numeric(18, 2)) ELSE NULL END) AS Average_Ticket_Price,
    SUM(CASE WHEN rda.Reason_for_Delay IS NOT NULL THEN 1 ELSE 0 END) AS Delayed_Journey_Count,
    COUNT(*) AS Total_Journey_Count,
    (SUM(CASE WHEN rda.Reason_for_Delay IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS Delay_Rate_Percentage,
    AVG(CASE WHEN rda.Reason_for_Delay IS NOT NULL AND ISDATE(rda.Arrival_Time) = 1 AND ISDATE(rda.Actual_Arrival_Time) = 1 
	THEN ABS(DATEDIFF(MINUTE, CAST(rda.Arrival_Time AS datetime), CAST(rda.Actual_Arrival_Time AS datetime))) ELSE NULL END) AS Average_Delay_Time_In_Minutes,
    SUM(CASE WHEN ISNUMERIC(rda.Price) = 1 THEN CAST(rda.Price AS numeric(18, 2)) ELSE 0 END) AS Total_Revenue,
    SUM(CASE WHEN rda.Refund_Request = 'Yes' AND ISNUMERIC(rda.Price) = 1 THEN CAST(rda.Price AS numeric(18, 2)) ELSE 0 END) AS Revenue_Loss
FROM
    Railway_Data_Analysis rda
GROUP BY
    rda.Railcard,
    rda.Departure_Station,
    rda.Arrival_Destination
ORDER BY
    Average_Delay_Time_In_Minutes DESC;

/* Journey Count and Performance:

High Journey Counts: 
Routes like London Kings Cross to York, London Paddington to Reading and London St Pancras to Birmingham New Street have high journey counts.

100% Delay Rate: 
All the routes show a 100% delay rate, indicating that every recorded journey experienced a delay, regardless of the route or railcard type.

Average Ticket Prices:
High Average Ticket Prices:
Some routes, such as Liverpool Lime Street to London Paddington and Manchester Piccadilly to London Euston and London Paddington, have higher average ticket prices.
Low Average Ticket Prices: 
Routes like Birmingham New Street to Wolverhampton and London Euston to Oxford have lower average ticket prices.

Delay Statistics:
High Average Delay Times: 
Routes like Birmingham New Street to London Euston and London Euston to Manchester Piccadilly have significant average delay times.
Low Average Delay Times:
Some routes have relatively lower average delay times, such as Birmingham New Street to Manchester Piccadilly and Reading to Didcot.

Revenue Analysis:
High Total Revenue: 
Routes like London Kings Cross to York and London Paddington to Oxford generate high total revenues.
High Revenue Loss: Some routes show significant revenue loss due to refunds, such as Liverpool Lime Street to London Euston.

Railcard Impact:
No Railcard (None): 
The majority of the routes listed fall under the "None" category, meaning they are not associated with any specific railcard. 
These routes still exhibit high journey counts and revenue.
Railcards (Adult, Disabled, Senior): Certain routes are linked to specific railcards. The route Liverpool Lime Street to London Euston.

Insights:
1. High-revenue routes tend to have higher average ticket prices and journey counts, even though they also suffer from delays.
2. The impact of railcards is visible in the revenue distribution and average ticket prices.
3. To enhance customer satisfaction and revenue, addressing the delay issues is crucial.
*/
-----------------------------------------------------------------------------------------------------------------------------

/* 7. Journey Delay Impact Analysis by Hour of Day
This query analyzes how delays vary across different hours of the day, calculating the average delay in minutes for each hour and 
identifying the peak hours for delays. */

WITH DelayedJourneys AS (
    SELECT 
        Departure_Time,
        Arrival_Time,
        Actual_Arrival_Time,
        DATEDIFF(MINUTE, CAST(Arrival_Time AS TIME), CAST(Actual_Arrival_Time AS TIME)) AS Delay_In_Minutes,
        Departure_Station,
        Arrival_Destination
    FROM 
        Railway_Data_Analysis
    WHERE 
        Journey_Status = 'Delayed'
)

SELECT 
    Departure_Station,
    Arrival_Destination,
    DATEPART(HOUR, CAST(Departure_Time AS TIME)) AS HourOfDay,
    AVG(Delay_In_Minutes) AS Average_Delay_Time_In_Minutes
FROM 
    DelayedJourneys
GROUP BY 
    Departure_Station,
    Arrival_Destination,
    DATEPART(HOUR, CAST(Departure_Time AS TIME))
ORDER BY 
    Departure_Station,
    Arrival_Destination,
    HourOfDay;

/* 
The results from the query indicate the average delay experienced during different hours of the day across all journeys in the dataset. 
The hours with the highest average delays are considered peak delay hours.
Analysis :
Early Morning (1 AM to 5 AM): 
There are delays during these hours, possibly due to staffing , maintenance activities, or late-night scheduling challenges.
Morning Peak (6 AM to 9 AM): 
This period often shows higher average delays, suggesting congestion or rush-hour effects.
Midday (10 AM to 3 PM): 
Delays tend to decrease during these hours, possibly indicating smoother operations or fewer peak-time pressures.
Afternoon to Evening (4 PM to 7 PM):
There can be an increase in delays again, possibly due to evening rush-hour congestion.
Late Night (8 PM onwards): 
Delays may vary depending on technical issue or staff shortage.
solution:
The best way to normalise this is to increase the staffing and implementing the latest network technologies.
 Providing passengers with insights into peak delay hours can help them plan their journeys more effectively, potentially avoiding peak
 congestion periods or allowing for extra travel time during high-delay hours.
*/