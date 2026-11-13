SELECT p.category, COUNT(*) AS reviews, ROUND(AVG(r.rating), 2) AS avg_rating
FROM f_review r
JOIN d_product p USING (product_key)
JOIN d_date d USING (date_key)
JOIN d_review_flag f USING (flag_key)
WHERE r.is_deleted = FALSE
AND r.src_version_ts >= now() - interval '1 day' AND f.is_duplicate = FALSE
GROUP BY p.category
ORDER BY reviews DESC;
