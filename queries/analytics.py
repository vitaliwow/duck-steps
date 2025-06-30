from enum_models import TableNames, OrderStatus


ANALYTIC_QUERIES = {
    TableNames.ANALYTICS_MOST_VALUABLE_CUSTOMERS: f"""
        SELECT 
            scd.customer_unique_id,
            SUM(scd.price) AS total_spent,
            COUNT(DISTINCT scd.order_id) AS total_orders,
            MAX(scd.order_delivered_customer_date) AS last_order_date,
            RANK() OVER (ORDER BY total_spent DESC) AS customer_rating
        FROM 
            {TableNames.STAGING_CUSTOMERS_DELIVERIES.value} scd
        WHERE scd.order_status = '{OrderStatus.DELIVERED.value}'
        GROUP BY 
        scd.customer_unique_id, 
        DATE_TRUNC('month', scd.order_delivered_customer_date)
        ORDER BY 
            customer_rating;
    """,
    TableNames.ANALYTICS_ROLLING_QUARTERS: f"""
        SELECT
            scd.customer_unique_id,
            DATE_TRUNC('month', scd.order_delivered_customer_date) AS month,
            SUM(price) AS monthly_total,
            AVG(monthly_total) OVER (
                PARTITION BY scd.customer_unique_id
                ORDER BY month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS rolling_quartal_avg
        FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value} scd
        GROUP BY    
            scd.customer_unique_id, 
            DATE_TRUNC('month', scd.order_delivered_customer_date)
        ORDER BY scd.customer_unique_id, month;
    """,
    TableNames.ANALYTICS_TOP_TEN_PRODUCTS_Q_SALES: f"""
        WITH last_quarter AS (
            SELECT 
                DATE_TRUNC('quarter', MAX(order_purchase_timestamp)) AS quarter_start
            FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value}
        ),
        quarterly_sales AS (
            SELECT 
                scd.product_id,
                SUM(scd.price) as total_quarter_sales
            FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value} scd
            CROSS JOIN last_quarter
            WHERE
                scd.order_purchase_timestamp >= last_quarter.quarter_start
                AND scd.order_purchase_timestamp < last_quarter.quarter_start + INTERVAL 1 QUARTER
            GROUP BY
                scd.product_id
        )
        SELECT 
            product_id,
            ROW_NUMBER() OVER (ORDER BY total_quarter_sales DESC) AS rank_by_quarter_sales,
            total_quarter_sales
        FROM quarterly_sales
        ORDER BY rank_by_quarter_sales
        LIMIT 10;
    """,
    TableNames.ANALYTICS_TOP_TEN_PRODUCTS_Q_BY_CATEGORY: f"""
        WITH last_quarter AS (
            SELECT 
                DATE_TRUNC('quarter', MAX(order_purchase_timestamp)) AS quarter_start
            FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value}
        ),
        category_product_sales AS (
            SELECT
                scd.product_category_name_english,
                scd.product_id,
                SUM(scd.price) AS total_sales,
                ROW_NUMBER() OVER (
                    PARTITION BY scd.product_category_name_english 
                    ORDER BY SUM(scd.price) DESC
                ) AS rank_by_category
            FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value} scd
            CROSS JOIN last_quarter
            WHERE
                scd.order_purchase_timestamp >= last_quarter.quarter_start
                AND scd.order_purchase_timestamp < last_quarter.quarter_start + INTERVAL 1 QUARTER
            GROUP BY
                scd.product_category_name_english,
                scd.product_id
        )
        SELECT
            product_category_name_english,
            product_id,
            total_sales,
            rank_by_category 
        FROM category_product_sales
        WHERE rank_by_category <= 10 AND product_category_name_english IS NOT NULL
        ORDER BY 
            product_category_name_english,
            rank_by_category;
    """,
    TableNames.ANALYTICS_RAISE_SALES_GRADIENT: f"""
    WITH top_5_categories AS (
        SELECT
            product_category_name_english as pcne,
            SUM(price) as total_sales
        FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES}
        GROUP BY
            product_category_name_english
        ORDER BY total_sales DESC
        LIMIT 5
    ),
    monthly_sales AS (
        SELECT
            DATE_TRUNC('month', order_purchase_timestamp) as month,
            product_category_name_english,
            SUM(price) as monthly_sales
        FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES}
        WHERE product_category_name_english IN (SELECT pcne FROM top_5_categories)
        GROUP BY 
            DATE_TRUNC('month', order_purchase_timestamp),
            product_category_name_english
    ),
    sales_with_growth AS (
        SELECT
            m.*,
            LAG(monthly_sales) OVER (PARTITION BY product_category_name_english ORDER BY month) as prev_month_sales,
            monthly_sales - LAG(monthly_sales) OVER (PARTITION BY product_category_name_english ORDER BY month) as sales_change,
            (monthly_sales - LAG(monthly_sales) OVER (PARTITION BY product_category_name_english ORDER BY month)) / 
            NULLIF(LAG(monthly_sales) OVER (PARTITION BY product_category_name_english ORDER BY month), 0) * 100 as growth_percentage
        FROM monthly_sales m
    )
    SELECT
        product_category_name_english as product_category,
        month,
        ROUND(growth_percentage, 2) as growth_percentage
    FROM sales_with_growth
    WHERE prev_month_sales IS NOT NULL
    ORDER BY
        product_category_name_english,
        month;
    """,
    TableNames.ANALYTICS_CUMULATIVE_PRODUCT_SALES: f"""
        SELECT
            DISTINCT scd.product_id,
            SUM(scd.price) as total_sales,
        FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value} scd
        GROUP BY 
            scd.product_id,
            DATE_TRUNC('year', scd.order_purchase_timestamp)
        QUALIFY DATE_TRUNC('year', scd.order_purchase_timestamp) = MAX(DATE_TRUNC('year', scd.order_purchase_timestamp)) OVER()
        ORDER BY total_sales DESC
    """,
    TableNames.ANALYTICS_DAILY_REBATES: f"""
        WITH last_month_cutoff AS (
            SELECT MAX(order_purchase_timestamp) - INTERVAL 1 MONTH AS cutoff_date
            FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value}
        ),
        last_month_order_purchases AS (
            SELECT 
                scd.order_id,
                scd.order_purchase_timestamp,
                SUM(scd.price) as order_price
            FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value} scd
            CROSS JOIN last_month_cutoff
            WHERE scd.order_purchase_timestamp >= last_month_cutoff.cutoff_date
            GROUP BY
                scd.order_id,
                scd.order_purchase_timestamp,
            ORDER BY scd.order_purchase_timestamp DESC
        )
        SELECT 
            DATE_TRUNC('day', last_month_order_purchases.order_purchase_timestamp) as day_of_month,
            ROUND(SUM(last_month_order_purchases.order_price), 2) as daily_rebates
        from last_month_order_purchases
        GROUP BY day_of_month;
    """,
}
