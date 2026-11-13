SELECT p.category, COUNT(*) AS reviews, ROUND(AVG(r.rating), 2) AS avg_rating
FROM f_review r
JOIN d_product p USING (product_key)
JOIN d_date d USING (date_key)
JOIN d_customer c USING (customer_key)
WHERE r.is_deleted = FALSE
AND c.skin_type = '건성' AND r.rating <= 2
GROUP BY p.category
ORDER BY reviews DESC;
