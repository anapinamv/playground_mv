SELECT count(distinct email), utm_campaign FROM oathkeeper.unique_signups
where redirected_to_path like "%energia/thank-you%" and created_at >= "2020-06-21"
group by utm_campaign
LIMIT 1000