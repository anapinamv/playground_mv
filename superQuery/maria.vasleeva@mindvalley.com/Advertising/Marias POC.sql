SELECT signup_business_name, collected_amount
  FROM
    `mv-etl-staging.reports.cohort` 
    where payment_time >= '2020-04-01' 
 group by signup_business_name, collected_amount