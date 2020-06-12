select * 
from `mindvalleyadvertisingai.pm_dashboard.last_signup_attribution_v2`
where product_bought=@product_bought
limit 100