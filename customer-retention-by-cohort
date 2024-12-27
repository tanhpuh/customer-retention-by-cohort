WITH 
  raw_day_performance_metric as (
    SELECT DISTINCT
      date_key,
      customer_id,
    FROM 
      `order_tiki_device_utm`
    WHERE 1=1
  ),
  --day--
  raw_user_retention_day as (
    SELECT
      date_key as retention_day,
      customer_id
    FROM 
      raw_day_performance_metric
    GROUP BY 
      1,2
  ),
  raw_user_first_day as (    
    SELECT
      MIN(date_key) as first_day,
      customer_id
    FROM 
      raw_day_performance_metric
    GROUP BY 
      2
  ),
  raw_user_log_retention_day as (
    SELECT
      a.customer_id as customer_id,
      first_day,
      retention_day
    FROM 
      raw_user_retention_day a
    LEFT JOIN 
      raw_user_first_day b 
    ON 
      a.customer_id = b.customer_id
  ),
  raw_nu_user_first_day as (
    SELECT
      first_day,
      COUNT(DISTINCT customer_id) as new_users
    FROM 
      raw_user_first_day
    GROUP BY 
      1
  ),
  raw_nu_user_retention_day as (
    SELECT
      first_day,
      retention_day,
      COUNT(DISTINCT customer_id) as retention_users
    FROM 
      raw_user_log_retention_day
    GROUP BY 
      1,2
  ),
  retention_day_data as (
    SELECT
      'day' as date_type,
      a.first_day as first_day,
      retention_day,
      new_users,
      retention_users,
      retention_users / new_users as retention_rate,
      DATE_DIFF(retention_day, a.first_day, day) as retention_day_no
    FROM 
      raw_nu_user_retention_day a
    LEFT JOIN 
      raw_nu_user_first_day b 
    ON 
      a.first_day = b.first_day
  ),
  --week--
  raw_week_performance_metric as (
    SELECT
      DATE_TRUNC(date_key, WEEK(MONDAY)) as date_key,
      customer_id
    FROM 
      raw_day_performance_metric
  ),
  raw_user_retention_week as (    
    SELECT
      date_key as retention_week,
      customer_id
    FROM 
      raw_week_performance_metric
    GROUP BY 
      1,2
  ),
  raw_user_first_week as (    
    SELECT
      MIN(date_key) as first_week,
      customer_id
    FROM 
      raw_week_performance_metric
    GROUP BY 
      2
  ),
  raw_user_log_retention_week as (
    SELECT
      a.customer_id as customer_id,
      first_week,
      retention_week
    FROM 
      raw_user_retention_week a
    LEFT JOIN 
      raw_user_first_week b 
    ON 
     a.customer_id = b.customer_id
  ),
  raw_nu_user_first_week as (
    SELECT
      first_week,
      COUNT(DISTINCT customer_id) as new_users
    FROM 
      raw_user_first_week
    GROUP BY 
      1
  ),
  raw_nu_user_retention_week as (
    SELECT
      first_week,
      retention_week,
      COUNT(DISTINCT customer_id) as retention_users
    FROM 
      raw_user_log_retention_week
    GROUP BY 
      1,2
  ),
  retention_week_data as (
    SELECT
      'week' as date_type,
      a.first_week as first_week,
      retention_week,
      new_users,
      retention_users,
      retention_users / new_users as retention_rate,
      DATE_DIFF(retention_week, a.first_week, WEEK) as retention_week_no
    FROM 
      raw_nu_user_retention_week a
    LEFT JOIN 
      raw_nu_user_first_week b 
    ON 
      a.first_week = b.first_week    
  ),
  --month--
  raw_month_performance_metric as (
    SELECT
      DATE_TRUNC(date_key, MONTH) as date_key,
      customer_id
    FROM 
      raw_day_performance_metric
  ),
  raw_user_retention_month as (    
    SELECT
      date_key as retention_month,
      customer_id
    FROM 
      raw_month_performance_metric
    GROUP BY 
      1,2
  ),
  raw_user_first_month as (    
    SELECT
      MIN(date_key) as first_month,
      customer_id
    FROM 
      raw_month_performance_metric
    GROUP BY 
      2
  ),
  raw_user_log_retention_month as (
    SELECT
      a.customer_id as customer_id,
      first_month,
      retention_month
    FROM 
      raw_user_retention_month a
    LEFT JOIN 
      raw_user_first_month b 
    ON 
      a.customer_id = b.customer_id
  ),
  raw_nu_user_first_month as (
    SELECT
      first_month,
      COUNT(DISTINCT customer_id) as new_users
    FROM 
      raw_user_first_month
    GROUP BY 
      1
  ),
  raw_nu_user_retention_month as (
    SELECT
      first_month,
      retention_month,
      COUNT(DISTINCT customer_id) as retention_users
    FROM 
      raw_user_log_retention_month
    GROUP BY 
      1,2
  ),
  retention_month_data as (
    SELECT
      'month' as date_type,
      a.first_month as first_month,
      retention_month,
      new_users,
      retention_users,
      retention_users / new_users as retention_rate,
      DATE_DIFF(retention_month, a.first_month, MONTH) as retention_month_no
    FROM 
      raw_nu_user_retention_month a
    LEFT JOIN 
      raw_nu_user_first_month b 
    ON 
      a.first_month = b.first_month
  )
  SELECT
    date_type,
    first_day as first_time,
    retention_day as retention_time,
    new_users,
    retention_users,   
    retention_rate,
    CONCAT('Day',' ',retention_day_no) as retention_time_no
  FROM 
    retention_day_data
  WHERE
    1=1
    AND first_day BETWEEN DATE_SUB(CURRENT_DATE('+7'),INTERVAL 40 DAY) AND CURRENT_DATE('+7')


  UNION ALL

  SELECT
    date_type,
    first_week as first_time,
    retention_week as retention_time,
    new_users,
    retention_users,   
    retention_rate,
    CONCAT('Week',' ',retention_week_no) as retention_time_no
  FROM 
    retention_week_data
  WHERE
    1=1
    AND first_week BETWEEN DATE_SUB(CURRENT_DATE('+7'),INTERVAL 14 WEEK) AND CURRENT_DATE('+7')

  UNION ALL

  SELECT
    date_type,
    first_month as first_time,
    retention_month as retention_time,
    new_users,
    retention_users,   
    retention_rate,
    CONCAT('Month',' ',retention_month_no) as retention_time_no
  FROM 
    retention_month_data
  WHERE
    1=1
    AND first_month BETWEEN DATE_SUB(CURRENT_DATE('+7'),INTERVAL 14 MONTH) AND CURRENT_DATE('+7')
