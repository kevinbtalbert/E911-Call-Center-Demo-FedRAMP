-- View some records
SELECT * FROM default.e911_calls LIMIT 10;

-- Average Response Time by Incident Type
SELECT 
  incident_type,
  ROUND(AVG(CAST(response_time_seconds AS DOUBLE)), 2) AS avg_response_time_secs
FROM default.e911_calls
GROUP BY incident_type
ORDER BY avg_response_time_secs DESC;

-- Top Incident Types by Volume
SELECT 
  incident_type, 
  COUNT(*) AS incident_count
FROM default.e911_calls
GROUP BY incident_type
ORDER BY incident_count DESC
LIMIT 10;

-- 911 Calls Categorized by Community Risk Buckets
SELECT 
  CASE 
    WHEN CAST(incident_risk_score AS INT) BETWEEN 0 AND 3 THEN 'Low'
    WHEN CAST(incident_risk_score AS INT) BETWEEN 4 AND 7 THEN 'Medium'
    WHEN CAST(incident_risk_score AS INT) BETWEEN 8 AND 10 THEN 'High'
    ELSE 'Unknown'
  END AS risk_category,
  COUNT(*) AS call_volume,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
FROM default.e911_calls
GROUP BY 
  CASE 
    WHEN CAST(incident_risk_score AS INT) BETWEEN 0 AND 3 THEN 'Low'
    WHEN CAST(incident_risk_score AS INT) BETWEEN 4 AND 7 THEN 'Medium'
    WHEN CAST(incident_risk_score AS INT) BETWEEN 8 AND 10 THEN 'High'
    ELSE 'Unknown'
  END
ORDER BY call_volume DESC;
