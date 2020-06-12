select email,sum(collected_amount) as revenue 
from `mindvalleyadvertisingai.pm_dashboard.last_signup_attribution_v2`
where product_bought=@product_bought
and payment_date ="2020-6-01"
group by email